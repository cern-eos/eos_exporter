#!/usr/bin/env bash

echo "Exporting STCI_ROOT_PATH=/eos/project/s/storage-ci/www"
export STCI_ROOT_PATH="/eos/project/s/storage-ci/www"
echo "Exporting EOS_CODENAME=eos-monitoring"
export EOS_CODENAME="eos-monitoring"

for BUILD_TYPE in "el-7" "el-8" "el-9"; do
    EXPORT_DIR_RPMS=${STCI_ROOT_PATH}/${EOS_CODENAME}/${BUILD_TYPE}/x86_64/
    EXPORT_DIR_SRPMS=${STCI_ROOT_PATH}/${EOS_CODENAME}/${BUILD_TYPE}/SRPMS/
    echo "Publishing for: ${BUILD_TYPE} in location: ${EXPORT_DIR_RPMS}"
    mkdir -p ${EXPORT_DIR_RPMS}
    mkdir -p ${EXPORT_DIR_SRPMS}
    cp ${BUILD_TYPE}_artifacts/SRPMS/*.src.rpm ${EXPORT_DIR_SRPMS}
    cp ${BUILD_TYPE}_artifacts/RPMS/x86_64/*.rpm ${EXPORT_DIR_RPMS}
    createrepo -q ${EXPORT_DIR_RPMS}
done
