package Data::Queue::Persistent;

use 5.008004;
use strict;
use warnings;
use Carp qw / croak /;
use DBI;

our $VERSION = '0.01';

our $schema = q{
    CREATE TABLE %s (
                     key VARCHAR(255) NOT NULL,
                     idx INTEGER UNSIGNED NOT NULL,
                     value BLOB,
                     PRIMARY KEY (key, idx)
                     )
    };

sub new {
    my ($class, %opts) = @_;

    my $dsn = delete $opts{dsn} or croak "No DSN speficied for Data::Queue::Persistent";
    my $username = delete $opts{username};
    my $pass = delete $opts{pass};

    my $key = delete $opts{id} or croak "No queue id defined";

    my $table = delete $opts{table} || 'persistent_queue';

    # connect to db
    my $dbh = DBI->connect($dsn, $username, $pass) or croak "Could not connect to database";

    my $self = {
        dbh => $dbh,
        q   => [],
        key => $key,
        table_name => $table,
    };

    bless $self, $class;

    $self->init;
    $self->load;

    return $self;
}

sub table_name { $_[0]->{table_name} }
sub dbh { $_[0]->{dbh} }
sub key { $_[0]->{key} }
sub q { $_[0]->{q} }
sub length { scalar @{$_[0]->{q}} }

sub all { @{$_[0]->{q}} }

# do a sql statement and die if it fails
sub do {
    my ($self, $sql, @vals) = @_;

    $self->dbh->do($sql, undef, @vals);
    croak $self->dbh->errstr if $self->dbh->err;
}

# initialize the storage
sub init {
    my ($self) = @_;

    croak "No table name defined" unless $self->table_name;

    # don't do anything if table already exists
    return if $self->table_exists;

    # table doesn't exist, create it
    my $sql = sprintf($schema, $self->table_name);
    $self->do($sql);
}

# load data from db
sub load {
    my $self = shift;

    my $table = $self->table_name or croak "No table name defined";
    die "Table $table does not exist." unless $self->table_exists;

    my $rows = $self->dbh->selectall_arrayref("SELECT idx, value FROM $table WHERE key=?", undef, $self->key);
    die $self->dbh->errstr if $self->dbh->err;

    return unless $rows && @$rows;

    $self->absorb_rows(@$rows);
}

sub absorb_rows {
    my ($self, @rows) = @_;

    foreach my $row (@rows) {
        my ($idx, $val) = @$row;
        last unless defined $idx && defined $val;

        $self->{q}->[$idx] = $val;
    }
}

# delete everything from the queue
sub empty {
    my ($self) = @_;

    my $table = $self->table_name;
    $self->do("DELETE FROM $table WHERE key=?", $self->key);

    $self->{q} = [];
}

sub table_exists {
    my $self = shift;
    # get table info, see if our table exists
    my @tables = $self->dbh->tables(undef, undef, $self->table_name, "TABLE");
    return @tables ? 1 : 0;
}

# add @vals to the queue
*add = \&unshift;
sub unshift {
    my ($self, @vals) = @_;

    my $idx = $self->length;

    # add to end of queue
    push @{$self->{q}}, @vals;

    my $key = $self->dbh->quote($self->key);
    my $table = $self->table_name;
    my $dbh = $self->dbh;

    $dbh->begin_work;
    my $sth = $dbh->prepare(qq[ INSERT INTO $table (key, idx, value) VALUES ($key, ?, ?) ]);

    foreach my $val (@vals) {
        $sth->execute($idx++, $val);
        if ($dbh->err) {
            die $dbh->errstr;
            $dbh->rollback;
        }
    }

    $dbh->commit;
}

# shift $count elements off the queue
*remove = \&shift;
sub shift {
    my ($self, $_count) = @_;

    my $count = defined $_count ? $_count : 1;

    # get vals by unshifting in-memory array
    my @vals;
    push @vals, shift @{$self->{q}} for 1 .. $count;

    my $table = $self->table_name;

    # unshift value at index 0
    $self->do("DELETE FROM $table WHERE key=? AND idx < ?", $self->key, $count);

    # shift other indices down
    $self->do("UPDATE $table SET idx = idx - ? WHERE key=?", $count, $self->key);

    return $vals[0] unless defined $_count;
    return @vals;
}

1;

__END__

=head1 NAME

Data::Queue::Persistent - Perisistent database-backed queue

=head1 SYNOPSIS

  use Data::Queue::Persistent;
  my $q = Data::Queue::Persistent->new(
    table => 'persistent_queue',
    dsn   => 'dbi:SQLite:dbname=queue.db',
    id    => 'testqueue',
  );
  $q->add('first', 'second', 'third', 'fourth');
  $q->remove;      # returns 'first'
  $q->remove(2);   # returns ('second', 'third')
  $q->empty;       # removes everything

=head1 DESCRIPTION

This is a simple module to keep a persistent queue around. It is just
a normal implementation of a queue, except it is backed by a database
so that when your program exits the data won't disappear.

=head2 EXPORT

None by default.

=head2 Methods

=over 4

=item * new(%opts)

Creates a new persistent data queue object. This will also initialize
the database storage, and load the saved queue data if it already
exists.

Options:

dsn: DSN for database connection

id: The ID of this queue. You can have multiple queues stored in the same table, distinguished by their IDs.

user: The username for database connection (optional)

pass: The password for database connection (optional)

table: The table name to use ('persistent_queue' by default)

=item * add(@items)

Adds a list of items to the queue

=item * remove($count)

Removes $count (1 by default) items from the queue and returns
them. Returns value if no $count specified, otherwise returns an array
of values.

=item * all

Returns all elements in the queue. Does not modify the queue.

=item * empty

Removes all elements from the queue

=item * unshift(@items)

Alias for C<add(@items)>

=item * shift($count)

Alias for C<remove($count)>

=back

=head1 SEE ALSO

Any data structures book

=head1 AUTHOR

Mischa Spiegelmock, E<lt>mspiegelmock@gmail.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007 by Mischa Spiegelmock

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.4 or,
at your option, any later version of Perl 5 you may have available.


=cut
