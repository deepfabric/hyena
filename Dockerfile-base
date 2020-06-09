FROM deepfabric/centos

RUN yum -y install boost-thread boost-system boost-filesystem glog gflags openblas-devel jemalloc \
    && yum clean all && rm -rf /var/cache/yum