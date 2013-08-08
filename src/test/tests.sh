export PATH=$PATH:/sbin

MACH=bindir
SRC1=block1
SRC2=block2

rm -f ${SRC1} ${SRC1}.chk* ${SRC1}.del.* ${SRC2} ${SRC2}.chk* ${SRC2}.del.*

SEQ=0
dd if=/dev/zero bs=32M count=1 > ${SRC1} 2> /dev/null
../${MACH}/ddplus -s ${SRC1} -c ${SRC1}.chk -x ${SRC1}.del.${SEQ} 2>> ${SRC2}.del.log
../${MACH}/ddcommit -a apply -t ${SRC2} -c ${SRC2}.chk -x ${SRC1}.del.${SEQ} >> ${SRC2}.del.log


let SEQ=SEQ+1
mkfs.ext3 -q -F ${SRC1}
../${MACH}/ddplus -s ${SRC1} -c ${SRC1}.chk -x ${SRC1}.del.${SEQ} 2>> ${SRC2}.del.log
../${MACH}/ddcommit -a apply -t ${SRC2} -c ${SRC2}.chk -x ${SRC1}.del.${SEQ} >> ${SRC2}.del.log


let SEQ=SEQ+1
dd if=/dev/zero bs=32M count=1 >> ${SRC1} 2> /dev/null
../${MACH}/ddplus -s ${SRC1} -c ${SRC1}.chk -x ${SRC1}.del.${SEQ} -z 2>> ${SRC2}.del.log
../${MACH}/ddcommit -a apply -t ${SRC2} -c ${SRC2}.chk -x ${SRC1}.del.${SEQ} >> ${SRC2}.del.log

let SEQ=SEQ+1
mkfs.ext3 -q -F ${SRC1}
../${MACH}/ddplus -s ${SRC1} -c ${SRC1}.chk -x ${SRC1}.del.${SEQ} -z 2>> ${SRC2}.del.log
../${MACH}/ddcommit -a apply -t ${SRC2} -c ${SRC2}.chk -x ${SRC1}.del.${SEQ} >> ${SRC2}.del.log
# md5sum ${SRC1} ${SRC1}.chk ${SRC1}.del.${SEQ}

S51=$(md5sum ${SRC1} | awk '{print $1}')
S52=$(md5sum ${SRC2} | awk '{print $1}')

## ls -la ${SRC1} ${SRC2}
## md5sum ${SRC1} ${SRC2}

if [ "${S51}" != "${S52}" ]; then   
  echo "Delta Fail"; 
  exit
else 
  echo "Delta OK"; 
  echo
fi

../${MACH}/ddplus -s ${SRC2} -c ${SRC2}.chk 2>> ${SRC2}.del.log
## ls -la ${SRC1}.chk ${SRC2}.chk
## md5sum ${SRC1}.chk ${SRC2}.chk
C51=$(md5sum ${SRC1}.chk | awk '{print $1}')
C52=$(md5sum ${SRC2}.chk | awk '{print $1}')

if [ "${C51}" != "${C52}" ]; then   
  echo "Create Check Fail"; 
  exit
else 
  echo "Create Check OK"; 
  echo
fi


rm -f ${SRC2} ${SRC1}.chk
../${MACH}/ddplus -s ${SRC1} -t ${SRC2} -c ${SRC1}.chk 2>> ${SRC2}.del.log
S51=$(md5sum ${SRC1} | awk '{print $1}')
S52=$(md5sum ${SRC2} | awk '{print $1}')

if [ "${S51}" != "${S52}" ]; then   
  echo "Local Target 1 Fail"; 
  exit
else 
  echo "Local Target 1 OK"; 
  echo
fi

rm -f ${SRC2} ${SRC1}.chk
mkfs.ext3 -q -F ${SRC1}
../${MACH}/ddplus -s ${SRC1} -t ${SRC2} -c ${SRC1}.chk 2>> ${SRC2}.del.log
S51=$(md5sum ${SRC1} | awk '{print $1}')
S52=$(md5sum ${SRC2} | awk '{print $1}')

if [ "${S51}" != "${S52}" ]; then   
  echo "Local Target 2 Fail"; 
  exit
else 
  echo "Local Target 2 OK"; 
  echo
fi

