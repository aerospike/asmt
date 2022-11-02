Name: asmt
Version: @VERSION@
Release: 1%{?dist}
Summary: The Aerospike Shared Memory Tool
License: Apache 2.0 license
Group: Application
BuildArch: @ARCH@
%description
ASMT provides a program for backing up and restoring the primary index of 
an Aerospike Database Enterprise Edition (EE).
%define _topdir dist
%define __spec_install_post /usr/lib/rpm/brp-compress

%package tools
Summary: The Aerospike Shared Memory Tool
Group: Applications
%description tools
Tools for use with the Aerospike database
%files
%defattr(-,aerospike,aerospike)
/opt/aerospike/bin/asmt
%defattr(-,root,root)
/usr/bin/asmt

%prep
ln -sf /opt/aerospike/bin/asmt %{buildroot}/usr/bin/asmt

%pre tools
echo Installing /opt/aerospike/asmt
if ! id -g aerospike >/dev/null 2>&1; then
        echo "Adding group aerospike"
        /usr/sbin/groupadd -r aerospike
fi
if ! id -u aerospike >/dev/null 2>&1; then
        echo "Adding user aerospike"
        /usr/sbin/useradd -r -d /opt/aerospike -c 'Aerospike server' -g aerospike aerospike
fi

%preun tools
if [ $1 -eq 0 ]
then
        echo Removing /opt/aerospike/asmt
fi
