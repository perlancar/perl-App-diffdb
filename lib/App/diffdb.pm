package App::diffdb;

# DATE
# VERSION

use 5.010001;
use strict;
use warnings;
use Log::ger;

our %SPEC;

our %colors = (
    table_line  => "\e[1m",
    rownum_line => "\e[36m",
    colnum_line => "\e[36m",
    delete_line => "\e[31m",
    insert_line => "\e[32m",
    delete_word => "\e[7m",
    insert_word => "\e[7m",
    reset       => "\e[0m",
);

sub __json_encode {
    state $json = do {
        require JSON::MaybeXS;
        JSON::MaybeXS->new->canonical(1);
    };
    $json->encode(shift);
}

sub _get_row {
    my ($self, $sth) = @_;
    my $row = $sth->fetchrow_hashref;
    return undef unless $row;
    __json_encode($row);
}

sub __print_table_line {
    my ($label1, $label2) = @_;
    say "$colors{table_line}---$label1$colors{reset}";
    say "$colors{table_line}+++$label2$colors{reset}";
}

sub __print_uni_hunk {
    my ($line1, $num_lines1, $line2, $num_lines2, $hunk) = @_;
    return unless $num_lines1 || $num_lines2;
    say "$colors{rownum_line}\@\@-$line1,$num_lines1 +$line2,$num_lines2$colors{reset}";
    print $hunk;
}

sub _print_table {
    my ($self, $dbh, $table, $label1, $label2, $type) = @_;
    my $sth = $dbh->prepare("SELECT * FROM \"$table\"");
    $sth->execute;

    __print_table_line($label1, $label2);
    while (my $row = $self->_get_row($sth)) {
        if ($type eq 'delete') {
            say "$colors{delete_line}-$row$colors{reset}";
        } else {
            say "$colors{insert_line}+$row$colors{reset}";
        }
    }
}

sub _diff_table {
    require Algorithm::Diff;

    my ($self, $table) = @_;

    # XXX we should sort by PK first
    my @rows1;
    {
        my $sth = $self->{dbh1}->prepare("SELECT * FROM \"$table\"");
        $sth->execute;
        while (my $row = $sth->fetchrow_hashref) {
            push @rows1, __json_encode($row);
        }
    }
    my @rows2;
    {
        my $sth = $self->{dbh2}->prepare("SELECT * FROM \"$table\"");
        $sth->execute;
        while (my $row = $sth->fetchrow_hashref) {
            push @rows2, __json_encode($row);
        }
    }

    my $ctx = $self->{num_context_lines};

    my $diff = Algorithm::Diff->new(\@rows1, \@rows2);
    $diff->Base(1);
    # form unified-diff hunk which can contain one or more hunks
    my @uni_hunk; # (line1, num_lines1, line2, num_lines2, hunk)
    while ($diff->Next) {
        next if $diff->Same;
        my ($min1, $max1, $min2, $max2) = $diff->Get(qw/Min1 Max1 Min2 Max2/);
        if (!@uni_hunk) {
            @uni_hunk = (1, 0, 1, 0, "");
        } else {
            if ($min1 - $ctx > $uni_hunk[0]+$uni_hunk[1]-1) {
                __print_uni_hunk(@uni_hunk);
                @uni_hunk = ($min1 - $ctx, 0, $min2 - $ctx, 0, "");
            } else {
                for my $i ($uni_hunk[0] .. $min1-$ctx) {
                    $uni_hunk[4] .= " $rows1[$i-1]\n";
                    $uni_hunk[1]++;
                    $uni_hunk[3]++;
                }
            }
        }
        if ($diff->Items(1)) {
            for my $i ($min1..$max1) {
                $uni_hunk[4] .= "-$rows1[$i-1]\n";
                $uni_hunk[1]++;
            }
        }
        if ($diff->Items(2)) {
            for my $i ($min2..$max2) {
                $uni_hunk[4] .= "+$rows2[$i-1]\n";
                $uni_hunk[3]++;
            }
        }
    }
    __print_uni_hunk(@uni_hunk);
}

sub _diff_db {
    require DBIx::Diff::Schema;

    my $self = shift;

    my @tables1 = DBIx::Diff::Schema::_list_tables($self->{dbh1});
    my @tables2 = DBIx::Diff::Schema::_list_tables($self->{dbh2});

    # for now, we'll ignore schemas
    for (@tables1, @tables2) { s/.+\.// }

    my @all_tables = do {
        my %mem;
        my @all_tables;
        for (@tables1, @tables2) {
            push @all_tables, $_ unless $mem{$_}++;
        }
        sort @all_tables;
    };

    for my $table (@all_tables) {
        my $in_db1 = grep { $_ eq $table } @tables1;
        my $in_db2 = grep { $_ eq $table } @tables2;
        if ($in_db1 && $in_db2) {
            $self->_diff_table($table);
        } elsif (!$in_db2) {
            if ($self->{new_table}) {
                $self->_print_table(
                    $self->{dbh1}, $table,
                    "db1/$table", "db2/$table (doesn't exist)",
                    'delete');
            } else {
                say "Only in db1: $table";
            }
        } else {
            if ($self->{new_table}) {
                $self->_print_table(
                    $self->{dbh2}, $table,
                    "db1/$table (doesn't exist)", "db2/$table",
                    'insert');
            } else {
                say "Only in db2: $table";
            }
        }
    }

    [200];
}

$SPEC{diffdb} = {
    v => 1.1,
    summary => 'Compare two databases, line by line',
    'description.alt.env.cmdline' => <<'_',

This utility compares two databases and displays the result as the familiar
colored unified-style diff.

_
    args => {
        dsn1 => {
            summary => 'DBI data source, '.
                'e.g. "dbi:SQLite:dbname=/path/to/db1.db"',
            schema => 'str*',
            tags => ['connection'],
            pos => 0,
        },
        dsn2 => {
            summary => 'DBI data source, '.
                'e.g. "dbi:SQLite:dbname=/path/to/db1.db"',
            schema => 'str*',
            tags => ['connection'],
            pos => 1,
        },
        user1 => {
            schema => 'str*',
            cmdline_aliases => {user=>{}, u=>{}},
            tags => ['connection'],
        },
        password1 => {
            schema => 'str*',
            cmdline_aliases => {password=>{}, p=>{}},
            tags => ['connection'],
            description => <<'_',

You might want to specify this parameter in a configuration file instead of
directly as command-line option.

_
        },
        user2 => {
            schema => 'str*',
            description => <<'_',

Will default to `user1` if `user1` is specified.

_
            tags => ['connection'],
        },
        password2 => {
            schema => 'str*',
            description => <<'_',

Will default to `password1` if `password1` is specified.

You might want to specify this parameter in a configuration file instead of
directly as command-line option.

_
            tags => ['connection'],
        },
        dbh1 => {
            summary => 'Alternative to specifying dsn1/user1/password1',
            schema => 'obj*',
            tags => ['connection', 'hidden-cli'],
        },
        dbh2 => {
            summary => 'Alternative to specifying dsn2/user2/password2',
            schema => 'obj*',
            tags => ['connection', 'hidden-cli'],
        },

        new_table => {
            schema => ['bool*', is=>1],
            cmdline_aliases => {N=>{}},
            tags => ['diff'],
            description => <<'_',

This is analogous to the `--new-file` (`-N`) *diff* option.

_
        },
        color => {
            schema => ['bool*'],
            tags => ['diff'],
        },
        num_context_lines => {
            schema => ['int*', min=>0],
            default => 3,
            tags => ['diff'],
        },

        # XXX add arg: include table(s) pos=>2 greedy=>1
        # XXX add arg: exclude table(s)
        # XXX add arg: include table pattern
        # XXX add arg: exclude table pattern
        # XXX add arg: include column(s)
        # XXX add arg: exclude column(s)
        # XXX add arg: include column pattern
        # XXX add arg: exclude column pattern
        # XXX add arg: table sort
        # XXX add column sort args
        # XXX add row sort args
        # XXX add arg: option to show row as lines, or single-line hash, or single-line array, or single-line CSV/TSV
        # XXX add arg: new_column
    },

    args_rels => {
        'req_one&' => [
            [qw/dsn1 dbh1/],
            [qw/dsn2 dbh2/],
        ],
    },

    links => [
        {url=>'prog:diff'},
    ],

    'cmdline.skip_format' => 1,
};
sub diffdb {
    require DBI;

    my %args = @_;
    my $self = bless {%args}, __PACKAGE__;

    unless ($self->{dbh1}) {
        $self->{dbh1} =
            DBI->connect($args{dsn1}, $args{user1}, $args{password1},
                         {RaiseError=>1});
    }
    unless ($self->{dbh2}) {
        $self->{dbh2} =
            DBI->connect($args{dsn2},
                         $args{user2} // $args{user1},
                         $args{password2} // $args{password1},
                         {RaiseError=>1});
    }

    $self->{color} //= $ENV{COLOR} // (-t STDOUT);
    unless ($self->{color}) {
        $colors{$_} = "" for keys %colors;
    }
    $self->_diff_db;
}

1;
#ABSTRACT:

=head1 SYNOPSIS

See included script L<diffdb>.


=head1 ENVIRONMENT

=head2 COLOR => bool

Set default for C<--color> option.
