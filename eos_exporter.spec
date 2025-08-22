#
# eos_exporter spec file
#
%define version 0.1.15

Name: eos_exporter
Summary: The Prometheus EOS exporter exposes EOS metrics.
Version: %{version}
Release: 1%{?dist}
License: AGPLv3
BuildRoot: %{_tmppath}/%{name}-buildroot
Group: CERN-IT/ST
BuildArch: x86_64
Source: %{name}-%{version}.tar.gz

BuildRequires: systemd

%description
This RPM provides a binary and a systemd unit to run the eos_exporter in the EOS instance's MGMs.

# Don't do any post-install weirdness, especially compiling .py files
%define __os_install_post %{nil}

%{?systemd_requires}
Requires: systemd

#Pre installation/upgrade of RPM section
%pre      

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
install -D -m 644 %{name}.unit %{buildroot}%{_unitdir}/%{name}.service

%clean
rm -rf %buildroot/

%files
%defattr(-,root,root,-)
/var/log/eos_exporter/
/opt/eos_exporter/bin/*
%{_unitdir}/%{name}.service

%post
%systemd_post %{name}.service

%preun
%systemd_preun %{name}.service

%postun
%systemd_postun_with_restart %{name}.service

%changelog
* Fri Aug 22 2025 Jan Iven <jan.iven@cern.ch> 0.1.16-1
- Restart after RPM update
* Wed Aug 20 2025 Pablo Medina Ramos <pablo.medina.ramos@cern.ch> 0.1.15-1
- Adding namespace cache hit rate metrics.
- remove obsolete StandardOutput= from systemd unit file
- stop building for el7
* Wed Oct 16 2024 Maria Arsuaga Rios <maria.arsuaga.rios@cern.ch> 0.1.14-1
- Adding EC categories for fsck
* Wed Aug 21 2024 Hugo Gonzalez Labrador <hugo.gonzalez.labrador@cern.ch> 0.1.13-1
- Fix CI by using new docker runners
* Tue Aug 20 2024 Hugo Gonzalez Labrador <hugo.gonzalez.labrador@cern.ch> 0.1.12-1
- Fix CI
* Tue Aug 20 2024 Hugo Gonzalez Labrador <hugo.gonzalez.labrador@cern.ch> 0.1.11-1
- Add quotas exporter
* Mon Jun 3 2024 Cedric Caffy <ccaffy@cern.ch> 0.1.8-1
- Adds new eos inspector metrics as access time volume and files, birthtime and cost per group.
- Adds qclient metrics
- Package name includes the OS distribution
* Thu Feb 8 2024 Hugo Gonzalez <gonzalhu@cern.ch> 0.1.7-1
- Adds new eos inspector collector and volume per layout metrics
* Mon Jan 22 2024 Roberto Valverde <rvalverd@cern.ch> 0.1.6-1
- Adds new eos inspector collector and volume per layout metrics
* Thu Nov 09 2023 Roberto Valverde <rvalverd@cern.ch> 0.1.5-1
- Add metric reset to do not report on removed node/fs
- Add eos fusex collector with mount info 
* Wed Aug 16 2023 Roberto Valverde <rvalverd@cern.ch> 0.1.4-1
- Stability improvements 
- Removal of eos_vs collector 
* Mon Jul 10 2023 Roberto Valverde <rvalverd@cern.ch> 0.1.3-1
- Bugfix: Fixes unmarshal errors when space nominal quota is not defined.
* Tue Mar 07 2023 Roberto Valverde <rvalverd@cern.ch> 0.1.2-1
- Bugfix: crash when category error is specified in fsck repair
* Mon Mar 06 2023 Roberto Valverde <rvalverd@cern.ch> 0.1.1-1
- Fsck collector uses now eos fsck stat instead of fsck report for performance reasons.
- Fsck does not report by filesystem anymore, for performance reasons.
* Fri Mar 03 2023 Roberto Valverde <rvalverd@cern.ch> 0.1.0-1
- Mgm url gathered from EOS_MGM_ALIAS, removes dependency of CERN domain
- Added eos fsck collector for exposing fsck metrics 
- Added missing metric freebytes@configRW on the space collector
- Updated  Reame 
* Tue Feb 03 2023 Roberto Valverde <rvalverd@cern.ch> 0.0.14-1
- Fixes problem of acumulation of eos who metrics
* Tue Jan 31 2023 Roberto Valverde <rvalverd@cern.ch> 0.0.13-1
- Added eos who metrics
- Added missing eos node metrics 
* Mon Oct 17 2022 Roberto Valverde <rvalverd@cern.ch> 0.0.12-1
- Added eos recycle and eos who collectors. 
* Thu Jun 24 2022 Aritz Brosa Iartza <aritz.brosa.iartza@cern.ch> 0.0.11-1
- Remove -a flag from eos ns stat (NS collector ~7s scrape time), excludes batch user info.
* Thu Jun 22 2022 Aritz Brosa Iartza <aritz.brosa.iartza@cern.ch> 0.0.10-1
- Fix NS collector, fix unmarshalling issues.
* Thu May 10 2022 Aritz Brosa Iartza <aritz.brosa.iartza@cern.ch> 0.0.9-1
- Add IO stat collector, with its metrics.
* Thu Apr 26 2022 Aritz Brosa Iartza <aritz.brosa.iartza@cern.ch> 0.0.8-1
- Introduce batch overload metrics.
* Thu Mar 09 2022 Aritz Brosa Iartza <aritz.brosa.iartza@cern.ch> 0.0.7-1
- Improve the release title for GitHub tagged-releases, and improve systemd unit logs.
* Thu Feb 22 2022 Aritz Brosa Iartza <aritz.brosa.iartza@cern.ch> 0.0.5-1
- First version that is not a pre-release with proper systemd unit.
* Thu Feb 17 2022 Aritz Brosa Iartza <aritz.brosa.iartza@cern.ch> 0.0.4-1
- First version with RPMs building enabled.

