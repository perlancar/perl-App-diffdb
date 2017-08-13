package App::diffdb;

# DATE
# VERSION

use 5.010001;
use strict;
use warnings;
use Log::ger;

our %SPEC;

our %colors = (
    delete_line => "\e[31m",
    insert_line => "\e[32m",
    delete_word => "\e[7m",
    insert_word => "\e[7m",
    reset       => "\e[0m",
);

sub _print_row {
}

sub _print_table {
    my ($dbh, $table, $type) = @_;
    my $sth = $dbh->prepare("SELECT * FROM ?");
    $sth->execute($table);
    if ($type eq 'delete') {
    } else {
    }
    $self->_print_row($sth);
    print $colors{reset}, "\n";
}

sub _diffdb {
    my $self = shift;

    my @tables1 = DBIx::Diff::Schema::_list_tables($self->{dbh1});
    my @tables2 = DBIx::Diff::Schema::_list_tables($self->{dbh2});

    my $diff = Algorithm::Diff->new(\@tables1, \@tables2);
    while ($diff->Next) {
        for my $same ($diff->Same) {
            $self->_diff_table($same);
        }
        for my $del ($diff->Items(1)) {
            if ($args{new_table}) {
                $self->_print_table($self->{dbh1}, $del, 'delete');
            } else {
                say "Only in db1: $del";
            }
        }
        for my $ins ($diff->Items(2)) {
            if ($args{new_table}) {
                $self->_print_table($self->{dbh2}, $ins, 'insert');
            } else {
                say "Only in db2: $ins";
            }
        }
    }
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

        # XXX add arg: include table(s) pos=>2 greedy=>1
        # XXX add arg: exclude table(s)
        # XXX add arg: include table pattern
        # XXX add arg: exclude table pattern
        # XXX add arg: include column(s)
        # XXX add arg: exclude column(s)
        # XXX add arg: include column pattern
        # XXX add arg: exclude column pattern
        # XXX add column sort args
        # XXX add row sort args
        # XXX add arg: option to show row as lines, or single-line hash, or single-line array, or single-line CSV/TSV
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
    require DBIx::Diff::Schema;

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
    $self->_diffdb;
}

1;
#ABSTRACT:

=head1 SYNOPSIS

See included script L<diffdb>.


=head1 ENVIRONMENT

=head2 COLOR => bool

Set default for C<--color> option.
