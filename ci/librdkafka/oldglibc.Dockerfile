# The build host for the C bundle: glibc 2.17, so nothing compiled here can reference a symbol newer
# than the 2.19 Kotlin/Native ships.
#
# manylinux2014 rather than manylinux_2_28: 2.28 already has C11 threads, and librdkafka would stop
# falling back to the tinycthread it bundles - which is exactly the fallback this route depends on.
FROM quay.io/pypa/manylinux2014_x86_64

# OpenSSL 3.x's Configure needs IPC::Cmd and this image's perl 5.16 does not carry it. Without it
# Configure dies with "Can't locate IPC/Cmd.pm", which reads as an OpenSSL problem rather than a
# missing perl module.
RUN yum install -y -q perl-IPC-Cmd perl-Data-Dumper && yum clean all
