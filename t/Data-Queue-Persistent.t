use strict;

BEGIN {
    my $skip;
    eval "use DBD::SQLite; 1;" or $skip = 1;
    use Test::More qw(no_plan);

    if ($skip) {
        warn "You must have DBD::SQLite installed the run the tests";
        exit;
    }

    use_ok('Data::Queue::Persistent');
};

our $testdb = 'test.db';

my $q = Data::Queue::Persistent->new(
                                     dsn => "dbi:SQLite:dbname=$testdb",
                                     id  => 'test',
                                     );

ok($q, "Created persistent queue with sqlite backend");

# make sure queue is empty (in case it loaded data from a previous test)
$q->empty;

# add to queue
my @vals = ('a', 'b', 'c', 'lol', 'dongs');
$q->add(@vals);
is_deeply([$q->all], \@vals, 'add');

is($q->shift, 'a', 'shift');

$q->add(1, 2, 3);

is_deeply([$q->shift(3)], ['b', 'c', 'lol'], 'multiple shift');

is_deeply([$q->shift(4)], ['dongs', 1, 2, 3], 'multiple shift');

$q->add(7, 9, 11);


# load a new queue, make sure data is the same
my $q2 = Data::Queue::Persistent->new(
                                      dsn => "dbi:SQLite:dbname=$testdb",
                                      id  => 'test',
                                      );

is(scalar $q2->all, 3, "loaded queue from db");
is_deeply([$q2->all], [$q->all], "loaded queue from db");
