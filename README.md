Iterates through a ZFS filesystem (using zdb) and constructs a list of all id->parent mappings.

Automatically saves that list in a pickle file.

Optionally prints out id's that match specificed parents.


```

$ zfs-index --help
Usage: zfs-index [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  dmu-dnode-l0
  parents


$ zfs-index parents --help
Usage: zfs-index parents [OPTIONS] POOLNAME [PARENTS]...

Options:
  --no-status
  --debug
  --exit-early INTEGER  (for testing)
  --help                Show this message and exit.


$ zfs-index dmu-dnode-l0 --help
Usage: zfs-index dmu-dnode-l0 [OPTIONS] POOLNAME

Options:
  --no-status
  --debug
  --exit-early INTEGER  (for testing)
  --help                Show this message and exit.


$ zpool status poolz0_2TB_A -v
  pool: poolz0_2TB_A
 state: ONLINE
status: One or more devices has experienced an error resulting in data
        corruption.  Applications may be affected.
action: Restore the file in question if possible.  Otherwise restore the
        entire pool from backup.
   see: http://zfsonlinux.org/msg/ZFS-8000-8A
  scan: scrub repaired 0B in 0 days 00:42:36 with 4 errors on Sun Jun 16 07:59:13 2019
config:

        NAME          STATE     READ WRITE CKSUM
        poolz0_2TB_A  ONLINE       0     0     0
          sdb         ONLINE       0     0     0

errors: Permanent errors have been detected in the following files:

        /pool/fs/sha3_256/c/3/b/a
        /pool/fs/sha3_256/3/a/7/1

$ stat /pool/fs/sha3_256/c/3/b/a /pool/fs/sha3_256/3/a/7/1
  File: /pool/fs/sha3_256/c/3/b/a
  Size: 153             Blocks: 77         IO Block: 16384  directory
Device: 26h/38d Inode: 346174      Links: 2
Access: (0755/drwxr-xr-x)  Uid: ( 1000/    user)   Gid: ( 1000/    user)
Access: 2019-05-23 21:45:14.835737062 -0700
Modify: 2019-06-15 18:22:07.659706432 -0700
Change: 2019-06-15 18:22:07.659706432 -0700
 Birth: -
  File: /pool/fs/sha3_256/3/a/7/1
  Size: 96              Blocks: 47         IO Block: 16384  directory
Device: 26h/38d Inode: 448582      Links: 2
Access: (0755/drwxr-xr-x)  Uid: ( 1000/    user)   Gid: ( 1000/    user)
Access: 2019-05-23 22:13:53.879675614 -0700
Modify: 2019-06-15 18:06:11.664732806 -0700
Change: 2019-06-15 18:06:11.664732806 -0700
 Birth: -


$ zfs-index parents pool/fs 346174 448582
gathering all id->parent mappings
looking for id's with parent(s): 346174 448582
346173 346174
448581 448582
452992 448582
462526 346174
506730 346174
593590 346174
687519 448582
<snip>
20121540 346174
20140237 346174
20194194 346174
20230492 346174
20235568 346174
20331813 346174
Done. 9238092 id->parent mappings saved in:
/root/.zfs_index/parent_map_pool/fs_1560922599_3417_.pickle

# of id's: 9238092
# of id's with no parent: 9
# of id's with parent: 9238083
# of unique parents: 69907


$ ls -alh /home/user/.zfs_index/pool/fs_1560759374_3128_.pickle
-rw-r--r-- 1 user user 1.8M Jun 17 01:17 /home/user/.zfs_index/pool/fs_1560759374_3128_.pickle


$ zfs-index dmu-dnode-l0 pool/fs
gathering all L0 entries from DMU dnode
Done. 615945 L0 dnodes saved in:7 id/sec

$ ls -alh /root/.zfs_index/L0_list_pool/fs_1560924609_4489_.pickle
-rw-r--r-- 1 root root 110M Jun 18 23:13 /root/.zfs_index/L0_list_pool/fs_1560924609_4489_.pickle



```

Requires:

   python 3.7+

   click: https://palletsprojects.com/p/click

   zfs: https://zfsonlinux.org
