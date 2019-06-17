Iterates through a ZFS filesystem (using zdb) and constructs a list of all id->parent mappings.

Automatically saves that list in a pickle.

Optionally prints out id's that match specificed parents.


```
$ zfs-parent-hunter --help
Usage: zfs-parent-hunter [OPTIONS] POOLNAME [PARENTS]...

Options:
  --status
  --debug
  --exit-early INTEGER  (for testing)
  --help                Show this message and exit.

$ zfs-parent-hunter --status poolz0_2TB_A/iridb_data_index 346174 448582 --exit-early 200000
gathering all id->parent mappings
looking for id's with parent(s): 346174 448582
346173 346174
448581 448582
452992 448582
462526 346174
506730 346174
checking id: 532020 @ 1896 id/sec

Exiting early after 200000 id's
Done. 200000 id->parent mappings saved in:
/home/user/.zfs_parent_hunter/parent_map_poolz0_2TB_A/iridb_data_index_1560759374.1333466_3128_.pickle

# of id's: 200000
# of id's with no parent: 9
# of id's with parent: 199991
# of unique parents: 62011

$ ls -alh /home/user/.zfs_parent_hunter/parent_map_poolz0_2TB_A/iridb_data_index_1560759374.1333466_3128_.pickle
-rw-r--r-- 1 user user 1.8M Jun 17 01:17 /home/user/.zfs_parent_hunter/parent_map_poolz0_2TB_A/iridb_data_index_1560759374.1333466_3128_.pickle

```

Requires:
    >= python 3.7
    click
