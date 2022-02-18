#
# eos_exporter spec file
#

Name: eos_exporter
Summary: The Prometheus EOS exporter exposes EOS metrics.
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
install -m 755 eos_exporter %buildroot/opt/eos_exporter/bin/eos_exporter

%clean
rm -rf %buildroot/

%files
%defattr(-,root,root,-)
/var/log/eos_exporter
/opt/eos_exporter/bin/*

#Pre installation/upgrade of RPM section
%pre      
  #Upgrading
  if [ $1 -eq 2 ]; then
    /usr/bin/systemctl stop %{pkgname}.service >/dev/null 2>&1 ||:
  fi

%post
%systemd_post %{pkgname}.service

%preun
%systemd_preun %{pkgname}.service
  #old package
  #uninstall
  if [ $1 -eq 0 ]; then
    /usr/bin/systemctl --no-reload disable %{pkgname}.service
    /usr/bin/systemctl stop %{pkgname}.service >/dev/null 2>&1 ||:
    /usr/bin/systemctl disable %{pkgname}.service
  
  fi
  if [ $1 -eq 1 ]; then
    /usr/bin/systemctl --no-reload disable %{pkgname}.service
    /usr/bin/systemctl stop %{pkgname}.service
  fi

%changelog
* Thu Feb 17 2022 Aritz Brosa Iartza <aritz.brosa.iartza@cern.ch> 0.0.4-1
- First version with RPMs building enabled.

