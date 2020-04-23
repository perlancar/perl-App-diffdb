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
    diff_command => {
        schema => 'str*', # XXX prog
        default => 'diff',
    },
    row_as => {
        schema => ['str*', in=>['json-one-line', 'json-card']], # XXX yaml, csv, tsv, ...
        default => 'json-one-line',
    },
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

our %args_diff_common = (
    include_columns => {
        'x.name.is_plural' => 1,
        'x.name.singular' => 'include_column',
        schema => ['array*', of=>'str*'], # XXX completion
        cmdline_aliases => {c=>{}},
        tags => ['category:column-selection'],
    },
    exclude_columns => {
        'x.name.is_plural' => 1,
        'x.name.singular' => 'exclude_column',
        schema => ['array*', of=>'str*'], # XXX completion
        cmdline_aliases => {C=>{}},
        tags => ['category:column-selection'],
    },

    order_by => {
        schema => ['str*'],
        tags => ['category:row-ordering'],
    },
);

our %args_diff_db = (
    include_tables => {
        'x.name.is_plural' => 1,
        'x.name.singular' => 'include_table',
        schema => ['array*', of=>'str*'], # XXX completion
        cmdline_aliases => {t=>{}},
        tags => ['category:table-selection'],
    },
    exclude_tables => {
        'x.name.is_plural' => 1,
        'x.name.singular' => 'exclude_table',
        schema => ['array*', of=>'str*'], # XXX completion
        cmdline_aliases => {T=>{}},
        tags => ['category:table-selection'],
    },
    sql => {
        summary => 'Compare the result of SQL select query, instead of tables',
        schema => 'str*',
        tags => ['category:table-selection'],
    },
);

our %args_diff_dbtable = (
    table1 => {
        schema => 'str*',
        pos => 2,
    },
    table2 => {
        schema => 'str*',
        pos => 3,
    },
    sql1 => {
        summary => 'Compare the result of SQL select query, instead of tables',
        schema => 'str*',
        tags => ['category:table-selection'],
    },
    sql2 => {
        summary => 'Compare the result of SQL select query, instead of tables',
        schema => 'str*',
        tags => ['category:table-selection'],
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

sub _write_query_result {
    my ($self, $sth, $fh) = @_;
    my $rownum = 0;
    while (1) {
        $rownum++;
        my $row = $self->_get_row($rownum, $sth);
        last unless defined $row;
        print $fh $row;
    }
}

sub _write_query_to_file {
    my ($self, $which_dbh, $query, $fname) = @_;

    my $dblabel = $which_dbh == 1 ? "db1" : "db2";
    my $dbh = $which_dbh == 1 ? $self->{dbh1} : $self->{dbh2};
    $fname //= "$self->{tempdir}/$dblabel.query$which_dbh";

    open my $fh, ">", $fname;

    my $sth = $dbh->prepare($query);
    $sth->execute;
    $self->_write_query_result($sth, $fh);

    close $fh;
    return $fname;
}

sub _write_table_to_file {
    require DBIx::Diff::Schema;

    my ($self, $which_dbh, $table) = @_;

    my $dblabel = $which_dbh == 1 ? "db1" : "db2";
    my $dbh = $which_dbh == 1 ? $self->{dbh1} : $self->{dbh2};
    my $fname = "$self->{tempdir}/$dblabel.".(defined $table ? $table : 'doesnt_exist');
    my $order_by = $self->{order_by} // '';

    unless (defined $table) {
        open my $fh, ">", $fname;
        close $fh;
        return $fname;
    }

    log_trace "Writing $dblabel table '$table' to temporary file '$fname' ...";

    my @columns;
  COLUMN:
    for my $column (DBIx::Diff::Schema::list_columns($dbh, $table)) {
        if ($self->{include_columns} && @{ $self->{include_columns} }) {
            next COLUMN unless grep { $column->{COLUMN_NAME} eq $_ } @{ $self->{include_columns} };
        }
        if ($self->{exclude_columns} && @{ $self->{exclude_columns} }) {
            next COLUMN if grep { $column->{COLUMN_NAME} eq $_ } @{ $self->{exclude_columns} };
        }
        push @columns, $column;
    }
    die "No columns to select for table '$table' ($dblabel)" unless @columns;

    unless (length $order_by) {
        my @indexes = grep { $_->{is_unique} }
            DBIx::Diff::Schema::list_indexes($dbh, $table);
        if (@indexes) {
            $order_by = join(",", map {qq($_)} @{ $indexes[0]{columns} });
        }
    }
    my $query =
        "SELECT ".join(",", map {qq($_->{COLUMN_NAME})} @columns).
        " FROM $table".($order_by ? " ORDER BY $order_by" : "");
    $self->_write_query_to_file($which_dbh, $query, $fname);
}

sub _diff_files {
    require IPC::System::Options;

    my ($self, $fname1, $fname2) = @_;
    IPC::System::Options::system(
        {log=>1, exit_code_success_criteria=>[0,1]},
        $self->{diff_command}, "-u",
        $fname1, $fname2,
    );
}

sub _diff_db {
    require DBIx::Diff::Schema;

    my $self = shift;

    if (defined $self->{sql}) {
        if ($self->{dbh1} == $self->{dbh2}) {
            return [304, "The same query from the same database"];
        }
        my $fname1 = $self->_write_query_to_file(1, $self->{sql});
        my $fname2 = $self->_write_query_to_file(2, $self->{sql});
        $self->_diff_files($fname1, $fname2);
        return [200];
    }

    if ($self->{dbh1} == $self->{dbh2}) {
        return [304, "The same table(s) from the same database"];
    }

    # now the case that is left is to diff one or more tables from two dbs

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

  TABLE:
    for my $table (@all_tables) {
        if ($self->{include_tables} && @{ $self->{include_tables} }) {
            unless (grep { $_ eq $table } @{ $self->{include_tables} }) {
                log_trace "Skipping table $table (not in include_tables)";
                next TABLE;
            }
        }
        if ($self->{exclude_tables} && @{ $self->{exclude_tables} }) {
            if (grep { $_ eq $table } @{ $self->{exclude_tables} }) {
                log_trace "Skipping table $table (in exclude_tables)";
                next TABLE;
            }
        }
        my $in_db1 = grep { $_ eq $table } @tables1;
        my $in_db2 = grep { $_ eq $table } @tables2;
        my $fname1 = $self->_write_table_to_file(1, $in_db1 ? $table : undef);
        my $fname2 = $self->_write_table_to_file(2, $in_db2 ? $table : undef);
        $self->_diff_files($fname1, $fname2);
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
        %args_diff_common,
        %args_diff_db,
    },

    "cmdline.skip_format" => 1,
};
sub diffdb {
    require DBI;
    require File::Temp;

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

    return [400, "Please specify at least dsn1/dbh1 AND dsn2/dbh2"]
        unless $self->{dbh1} && $self->{dbh2};

    $self->{tempdir} = File::Temp::tempdir(CLEANUP => $ENV{DEBUG});
    $self->{diff_command} //= 'diff';
    $self->{row_as} //= 'json-one-line';
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
        %args_diff_common,
        %args_diff_db,
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

sub _diff_dbtable {
    my $self = shift;

    if (defined $self->{sql1}) {
        my $fname1 = $self->_write_query_to_file(1, $self->{sql1});
        my $fname2 = $self->_write_query_to_file(2, $self->{sql2});
        $self->_diff_files($fname1, $fname2);
        return [200];
    }

    if (defined $self->{table1}) {
        if ($self->{dbh1} == $self->{dbh2} && $self->{table1} eq $self->{table2}) {
            return [304, "The same table from the same database"];
        }
        my $fname1 = $self->_write_table_to_file(1, $self->{table1});
        my $fname2 = $self->_write_table_to_file(2, $self->{table2});
        $self->_diff_files($fname1, $fname2);
        return [200];
    }

    # should not be reached
    die "Please specify either SQL or table";
}

$SPEC{diffdbtable} = {
    v => 1.1,
    summary => 'Compare two database tables, line by line',
    'description.alt.env.cmdline' => <<'_',

This utility compares two database tables and displays the result as the
familiar colored unified-style diff.

_
    args => {
        %args_common,
        %args_connect_dbi,
        %args_diff_common,
        %args_diff_dbtable,
    },

    "cmdline.skip_format" => 1,

    args_rels => {
    },

    links => [
        {url=>'prog:diff'},
    ],
};
sub diffdbtable {
    require DBI;
    require File::Temp;

    my %args = @_;
    my $self = bless {%args}, __PACKAGE__;

    unless ($self->{dbh1}) {
        $self->{dbh1} =
            DBI->connect($args{dsn1}, $args{user1}, $args{password1},
                         {RaiseError=>1});
    }

    unless ($self->{dbh2}) {
        if ($args{dsn2}) {
            $self->{dbh2} =
                DBI->connect($args{dsn2},
                             $args{user2} // $args{user1},
                             $args{password2} // $args{password1},
                             {RaiseError=>1});
        } else {
            $self->{dbh2} = $self->{dbh1};
        }
    }

    return [400, "Please specify at least dsn1/dbh1"]
        unless $self->{dbh1} && $self->{dbh2};

    $self->{tempdir} = File::Temp::tempdir(CLEANUP => $ENV{DEBUG});
    $self->{diff_command} //= 'diff';
    $self->{row_as} //= 'json-one-line';
    $self->{table2} //= $self->{table1};
    $self->{sql2} //= $self->{sql1};
    $self->_diff_dbtable;
}

$SPEC{diffdbtable_sqlite} = {
    v => 1.1,
    summary => 'Compare two SQLite database tables, line by line',
    'description.alt.env.cmdline' => <<'_',

This utility compares two SQLite database tables and displays the result as the
familiar colored unified-style diff.

_
    args => {
        %args_common,
        %args_connect_sqlite,
        %args_diff_common,
        %args_diff_dbtable,
    },

    "cmdline.skip_format" => 1,
};
sub diffdbtable_sqlite {
    my %args = @_;

    my $dsn1 = "dbi:SQLite:dbname=".delete($args{dbpath1});
    my $dsn2 = "dbi:SQLite:dbname=".delete($args{dbpath2}) if defined $args{dbpath2};
    diffdbtable(
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


=head1 SEE ALSO

L<diff-db-schema> from L<App::DiffDBSchemaUtils> which presents the result
structure from L<DBIx::Diff::Schema> directly.
