#
# eos_exporter spec file
#

Name: eos_exporter
Summary: The Prometheus EOs exporter exposes EOS metrics.
Version: 0.0.4
Release: 1
License: AGPLv3
BuildRoot: %{_tmppath}/%{name}-buildroot
Group: CERN-IT/ST
BuildArch: x86_64
Source: %{name}-%{version}.tar.gz

%description
This RPM provides a binary and a systemd unit to run the eos_exporter in the EOS instance's MGMs.

# Don't do any post-install weirdness, especially compiling .py files
%define __os_install_post %{nil}

%{?systemd_requires}
BuildRequires: systemd

%prep
%setup -n %{name}-%{version}

%install
# server versioning

# installation
rm -rf %buildroot/
mkdir -p %buildroot/usr/local/bin
mkdir -p %buildroot/opt/eos_exporter/bin
mkdir -p %buildroot/etc/logrotate.d
mkdir -p %buildroot/var/log/eos_exporter
install -m 755 cernboxcop %buildroot/opt/eos_exporter/bin/eos_exporter

%clean
rm -rf %buildroot/

%preun

%post
%systemd_post eos-exporter.service

%files
%defattr(-,root,root,-)
/etc/
/etc/logrotate.d/eos_exporter
/var/log/eos_exporter
/opt/eos_exporter/bin/*


%changelog
* Thu Feb 17 2022 Aritz Brosa Iartza <aritz.brosa.iartza@cern.ch> 0.0.4-1
- First version with RPMs building enabled.

