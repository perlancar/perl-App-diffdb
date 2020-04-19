package App::diffdb;

# AUTHORITY
# DATE
# DIST
# VERSION

use 5.010001;
use strict;
use warnings;
use Log::ger;

#use File::Slurper qw(read_text write_text);

our %SPEC;

our %args_common = (
    action => {
        schema => ['str*', in=>[
            'list_tables1',
            'list_tables2',
            'diff_db',
        ]],
        default => 'diff_db',
        cmdline_aliases => {
            'tables1' => {
                summary => 'Shortcut for --action=list_tables1',
                is_flag=>1,
                code => sub { $_[0]{action} = 'list_tables1' },
            },
            'tables2' => {
                summary => 'Shortcut for --action=list_tables2',
                is_flag=>1,
                code => sub { $_[0]{action} = 'list_tables2' },
            },
        },
    },
    diff_command => {
        schema => 'str*', # XXX prog
        default => 'diff',
    },
    row_as => {
        schema => ['str*', in=>['json-one-line', 'json-card']], # XXX yaml, csv, tsv, ...
        default => 'json-card',
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
);

our %args_connect_dbi = (
    dsn1 => {
        summary => 'DBI data source, '.
            'e.g. "dbi:SQLite:dbname=/path/to/db1.db"',
        schema => 'str*',
        tags => ['category:connection'],
        pos => 0,
    },
    dsn2 => {
        summary => 'DBI data source, '.
            'e.g. "dbi:SQLite:dbname=/path/to/db1.db"',
        schema => 'str*',
        tags => ['category:connection'],
        pos => 1,
    },
    user1 => {
        schema => 'str*',
        cmdline_aliases => {user=>{}, u=>{}},
        tags => ['category:connection'],
    },
    password1 => {
        schema => 'str*',
        cmdline_aliases => {password=>{}, p=>{}},
        tags => ['category:connection'],
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
        tags => ['category:connection'],
    },
    password2 => {
        schema => 'str*',
        description => <<'_',

Will default to `password1` if `password1` is specified.

You might want to specify this parameter in a configuration file instead of
directly as command-line option.

_
        tags => ['category:connection'],
    },
    dbh1 => {
        summary => 'Alternative to specifying dsn1/user1/password1',
        schema => 'obj*',
        tags => ['category:connection', 'hidden-cli'],
    },
    dbh2 => {
        summary => 'Alternative to specifying dsn2/user2/password2',
        schema => 'obj*',
        tags => ['category:connection', 'hidden-cli'],
    },
);

our %args_connect_sqlite = (
    dbpath1 => {
        summary => 'First SQLite database file',
        schema => 'filename*',
        tags => ['category:connection'],
        pos => 0,
    },
    dbpath2 => {
        summary => 'Second SQLite database file',
        schema => 'filename*',
        tags => ['category:connection'],
        pos => 1,
    },
);

sub __json_encode {
    state $json = do {
        require JSON::MaybeXS;
        JSON::MaybeXS->new->allow_nonref(1)->canonical(1);
    };
    $json->encode(shift);
}

sub _get_row {
    my ($self, $rownum, $sth) = @_;
    my $row = $sth->fetchrow_hashref;
    return undef unless $row;
    if ($self->{row_as} eq 'json-one-line') {
        my $res = __json_encode($row);
        $res .= "\n" unless $res =~ /\R\z/;
        $res;
    } elsif ($self->{row_as} eq 'json-card') {
        my $res = join(
            "",
            "Row #$rownum:\n",
            (map { "  $_: ".__json_encode($row->{$_})."\n" } sort keys %$row),
            "---\n",
        );
    } else {
        die "Uknown 'row_as' value '$self->{row_as}'";
    }
}

sub _diff_table {
    require IPC::System::Options;

    my ($self, $table, $table1_exists, $table2_exists) = @_;

    my $fname1 = "$self->{tempdir}/db1.$table".($table1_exists ? '' : '.doesnt_exist');
  CREATE_FILE1: {
        open my $fh, ">", $fname1;
        last unless $table1_exists;

        # XXX sort by PK
        my $sth = $self->{dbh1}->prepare("SELECT * FROM \"$table\"");
        $sth->execute;
        my $rownum = 0;
        while (1) {
            $rownum++;
            my $row = $self->_get_row($rownum, $sth);
            last unless defined $row;
            print $fh $row;
        }
    }

    my $fname2 = "$self->{tempdir}/db2.$table".($table2_exists ? '' : '.doesnt_exist');
  CREATE_FILE2: {
        open my $fh, ">", $fname2;
        last unless $table2_exists;

        # XXX sort by PK
        my $sth = $self->{dbh2}->prepare("SELECT * FROM \"$table\"");
        $sth->execute;
        my $rownum = 0;
        while (1) {
            $rownum++;
            my $row = $self->_get_row($rownum, $sth);
            last unless defined $row;
            print $fh $row;
        }
    }

    IPC::System::Options::system(
        {log=>1},
        $self->{diff_command}, "-u",
        $fname1, $fname2,
    );
}

sub _diff_db {
    require DBIx::Diff::Schema;

    my $self = shift;

    my @tables1 = DBIx::Diff::Schema::list_tables($self->{dbh1});
    my @tables2 = DBIx::Diff::Schema::list_tables($self->{dbh2});

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
            $self->_diff_table($table, 1, 1);
        } elsif (!$in_db2) {
            $self->_diff_table($table, 1, 0);
        } else {
            $self->_diff_table($table, 0, 1);
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
        %args_common,
        %args_connect_dbi,
    },

    "cmdline.skip_format" => 1,

    args_rels => {
    },

    links => [
        {url=>'prog:diff'},
    ],
};
sub diffdb {
    require DBI;
    require File::Temp;

    my %args = @_;
    my $action = $args{action};
    my $self = bless {%args}, __PACKAGE__;

    unless ($self->{dbh1}) {
        $self->{dbh1} =
            DBI->connect($args{dsn1}, $args{user1}, $args{password1},
                         {RaiseError=>1});
    }
    if ($action eq 'list_tables1') {
        require DBIx::Diff::Schema;
        for (DBIx::Diff::Schema::list_tables($self->{dbh1})) {
            s/.+\.//; # ignore schema for now
            print "$_\n";
        }
        return [200];
    }

    unless ($self->{dbh2}) {
        $self->{dbh2} =
            DBI->connect($args{dsn2},
                         $args{user2} // $args{user1},
                         $args{password2} // $args{password1},
                         {RaiseError=>1});
    }
    if ($action eq 'list_tables2') {
        require DBIx::Diff::Schema;
        for (DBIx::Diff::Schema::list_tables($self->{dbh2})) {
            s/.+\.//; # ignore schema for now
            print "$_\n";
        }
        return [200];
    }

    return [400, "Please specify dsn1/dbh1 and dsn2/dbh2"]
        unless $self->{dbh1} && $self->{dbh2};

    $self->{tempdir} = File::Temp::tempdir(CLEANUP => $ENV{DEBUG});
    $self->{diff_command} = $args{diff_command} // 'diff';
    $self->{row_as} = $args{row_as} // 'one-line-json';

    $self->_diff_db;
}

$SPEC{diffdb_sqlite} = {
    v => 1.1,
    summary => 'Compare two SQLite databases, line by line',
    'description.alt.env.cmdline' => <<'_',

This utility compares two SQLite databases and displays the result as the
familiar colored unified-style diff.

_
    args => {
        %args_common,
        %args_connect_sqlite,
    },

    "cmdline.skip_format" => 1,

    args_rels => {
    },

    links => [
        {url=>'prog:diff'},
    ],
};
sub diffdb_sqlite {
    my %args = @_;

    my $dsn1 = defined $args{dbpath1} ? "dbi:SQLite:dbname=".delete($args{dbpath1}) : undef;
    my $dsn2 = defined $args{dbpath2} ? "dbi:SQLite:dbname=".delete($args{dbpath2}) : undef;
    diffdb(
        %args,
        dsn1 => $dsn1,
        dsn2 => $dsn2,
    );
}

1;
#ABSTRACT:

=head1 SYNOPSIS

See included scripts L<diffdb>, L<diffdb-sqlite>, ...


=head1 ENVIRONMENT

=head2 DEBUG

Bool. If set to true, temporary directory is not cleaned up at the end of
runtime.


=head1 prepend:SEE ALSO

L<diff-db-schema> from L<App::DiffDBSchemaUtils> which presents the result
structure from L<DBIx::Diff::Schema> directly.
