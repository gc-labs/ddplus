ddplus
======
Based on the ddless utility - See https://www3.amherst.edu/~swplotner/iscsitarget/

ddless - summary 
================
ddless allows the syncing of local block devices.
The clever part is that it stores a checksum of each block it copies and only copies the changes
the next time you run it. And, because it runs at the block level, it doesn't care about the
filesystem inside, so you can backup Windows/Linux/BSD VMs with the same tool. The "downside" is 
that you need to do a full read through of the source volume each time and you need to keep a 
checksum file, 1M for each 2GB of source volume. But it doesn't suffer from the rsync issue where
you rename a directory and have to copy that whole directory again.


why ddplus?
===========
The lack of network capability was a problem for me, so I extended it to produce a difference file 
(like rsync can) which can be copied to the remote backup system(s) and "applied".
This results in a bare-metal backup "image" of the source data which can be mounted and inspected.


when to use ddplus
==================
The scenario I have is:
1) Large amounts of Virtual Machine data stored in Logical Volumes.
2) Small amounts of bandwidth to the backup sites.
