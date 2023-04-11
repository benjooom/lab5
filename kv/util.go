package kv

import "hash/fnv"

/// This file can be used for any common code you want to define and separate
/// out from server.go or client.go

func GetShardForKey(key string, numShards int) int {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return int(hasher.Sum32())%numShards + 1
}

func difference(s1, s2 []int) []int {
	diff := make([]int, 0)
	map1 := make(map[int]bool)
	map2 := make(map[int]bool)
	for _, val := range s1 {
		map1[val] = true
	}
	for _, val := range s2 {
		map2[val] = true
	}
	for key := range map1 {
		if _, ok := map2[key]; !ok {
			diff = append(diff, key)
		}
	}
	return diff
}
