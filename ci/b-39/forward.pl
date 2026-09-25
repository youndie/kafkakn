#!/usr/bin/perl
# Inside the arm64 container: 127.0.0.1:<port> -> host.docker.internal:<port>, for each port given.
#
# The broker's listeners advertise 127.0.0.1, so a client that bootstraps through any other address is
# sent back to 127.0.0.1 by the broker's metadata. Inside a container that is the container's own
# loopback. Docker Desktop's host networking is a setting that is off here and not this script's to
# turn on. So the container's loopback is made to lead to the Mac's, where ci/b-39/run.sh's SSH tunnel
# leads on to the broker.
#
# Perl because it is in the base image: nothing is installed into the container, so what runs the
# binary is a stock ubuntu:24.04 plus this file.
use strict;
use warnings;
use IO::Socket::INET;
use IO::Select;

my $upstream = $ENV{FORWARD_TO} // 'host.docker.internal';
$SIG{CHLD} = 'IGNORE';

for my $port (@ARGV) {
    my $listener = IO::Socket::INET->new(
        LocalAddr => '127.0.0.1', LocalPort => $port, Listen => 64, ReuseAddr => 1,
    ) or die "listen $port: $!";
    next if fork;
    while (my $client = $listener->accept) {
        next if fork;
        my $server = IO::Socket::INET->new(PeerAddr => $upstream, PeerPort => $port)
            or die "connect $upstream:$port: $!";
        pump($client, $server);
        exit 0;
    }
    exit 0;
}
sleep while 1;

sub pump {
    my ($a, $b) = @_;
    my $select = IO::Select->new($a, $b);
    while (my @ready = $select->can_read) {
        for my $from (@ready) {
            my $to = $from == $a ? $b : $a;
            my $n = sysread($from, my $buffer, 65536);
            return unless $n;
            my $off = 0;
            while ($off < $n) {
                my $w = syswrite($to, $buffer, $n - $off, $off);
                return unless defined $w;
                $off += $w;
            }
        }
    }
}
