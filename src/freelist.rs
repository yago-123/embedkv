use std::cmp::Ordering;

#[derive(Debug, Eq, PartialEq, PartialOrd, Clone)]
pub struct FreeSpace {
    pub space: usize,
    pub cursor: usize,
}

impl Ord for FreeSpace {
    fn cmp(&self, other: &Self) -> Ordering {
        let order = self.space.cmp(&other.space);
        if order == Ordering::Equal {
            // todo(): refine the ordering
        }

        return order
    }
}

impl FreeSpace {
    fn is_neighbour_of(&self, spot: &FreeSpace) -> bool {
        let func_is_neighbour = | nb1: &FreeSpace, nb2: &FreeSpace | -> bool {
            nb1.cursor > 0 && (nb1.cursor == nb2.cursor + nb2.space)
        };

        return func_is_neighbour(self, spot) || func_is_neighbour(spot, self);
    }

    fn merge_with(&self, spot: &FreeSpace) -> FreeSpace {
        FreeSpace {
            cursor: self.cursor.min(spot.cursor),
            space: self.space + spot.space,
        }
    }
}

pub struct FreeList {
    list: Vec<FreeSpace>,
    total_free_space: usize,
}

impl FreeList {
    pub fn new() -> Self {
        Self {
            list: Vec::new(),
            total_free_space: 0,
        }
    }

    pub fn insert_free_space(&mut self, cursor: usize, space: usize) {
        let value = FreeSpace { cursor, space };
        let pos = match self.list.binary_search(&value) {
            Ok(pos) | Err(pos) => pos,
        };

        self.total_free_space += space;
        self.list.insert(pos, value);
    }

    pub fn retrieve_free_space(&mut self, space: usize) -> Option<usize> {
        let space_cursor = FreeSpace{space: space, cursor: 0};

        if let Some(val) = self.retrieve_equal_or_bigger_than(&space_cursor) {
            self.total_free_space -= val.space;
            return Some(val.cursor)
        }

        return None
    }

    pub fn compact(&mut self) {
        let mut new_list: Vec<FreeSpace> = vec![];
        let mut already_merged: Vec<usize> = vec![];

        // re-sort by cursor so we can execute compact() only once
        self.list.sort_by(|a, b| a.cursor.cmp(&b.cursor));

        // range over all the elements in the list, find all the neighbours and merge them into
        // a single new list of free spaces. The new free space is calculated on the fly so
        // we only need one iteration for each element
        for (x, fs1) in self.list.iter().enumerate() {
            if already_merged.contains(&x) {
                continue
            }


            let mut tmp_fs = fs1.clone();
            // check for neighbours and merge those that fit
            for (y, fs2) in self.list.iter().enumerate().skip(x + 1) {
                if  tmp_fs.is_neighbour_of(fs2) && !already_merged.contains(&y) {
                    tmp_fs = tmp_fs.merge_with(fs2);
                    already_merged.push(y);
                }
            }

            // append the new free space with all the spots that matched
            new_list.push(tmp_fs);
        }

        // sort the list by space and replace the old free list with the already compacted list
        new_list.sort();
        self.list = new_list;
    }

    fn retrieve_equal_or_bigger_than(&mut self, expected_amount: &FreeSpace) -> Option<FreeSpace> {
        let mut claimed;

        // search for the first item in the list that have equal or bigger space available
        let pos = match self.list.binary_search(expected_amount) {
            Ok(pos) => pos,
            Err(pos) if pos < self.list.len() => pos,
            _ => return None,
        };

        claimed = self.list.remove(pos);

        // store again the free space if the space claimed has been bigger than the space
        // that is going to be filled
        if claimed.space > expected_amount.space {
            let free_space = FreeSpace {
                space: claimed.space - expected_amount.space,
                cursor: claimed.cursor + expected_amount.space,
            };

            self.list.insert(pos, free_space);
        }

        // update the real space that is going to be retrieved (just for correctness)
        claimed.space = expected_amount.space;

        Some(claimed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_free_space() {
        // insert one element
        let mut free_list = FreeList::new();
        free_list.insert_free_space(0, 10);
        assert_eq!(free_list.list, vec![FreeSpace {space: 10, cursor: 0}]);

        // insert free space at the beginning
        free_list.insert_free_space(10, 5);
        assert_eq!(
            free_list.list,
            vec![FreeSpace {space: 5, cursor: 10}, FreeSpace {space: 10, cursor: 0}]
        );

        // insert free space at the end
        free_list.insert_free_space(20, 80);
        assert_eq!(
            free_list.list,
            vec![
                FreeSpace {space: 5, cursor: 10},
                FreeSpace {space: 10, cursor: 0},
                FreeSpace {space: 80, cursor: 20},
            ]
        );

        // insert same space but different cursor
        free_list.insert_free_space(30, 8);
        assert_eq!(
            free_list.list,
            vec![
                FreeSpace {space: 5, cursor: 10},
                FreeSpace {space: 8, cursor: 30},
                FreeSpace {space: 10, cursor: 0},
                FreeSpace {space: 80, cursor: 20},
            ]
        );

        // insert cursor already present in the list with different space (should not happen in theory)
        free_list.insert_free_space(0, 11);
        assert_eq!(
            free_list.list,
            vec![
                FreeSpace {space: 5, cursor: 10},
                FreeSpace {space: 8, cursor: 30},
                FreeSpace {space: 10, cursor: 0},
                FreeSpace {space: 11, cursor: 0},
                FreeSpace {space: 80, cursor: 20},
            ]
        );

        // insert same space and same cursor (can't happen in theory)
        free_list.insert_free_space(10, 5);
        assert_eq!(
            free_list.list,
            vec![
                FreeSpace {space: 5, cursor: 10},
                FreeSpace {space: 5, cursor: 10},
                FreeSpace {space: 8, cursor: 30},
                FreeSpace {space: 10, cursor: 0},
                FreeSpace {space: 11, cursor: 0},
                FreeSpace {space: 80, cursor: 20},
            ]
        );

    }

    #[test]
    fn test_retrieve_free_space() {
        let mut free_list = FreeList::new();

        // retrieve free space when there are no values stored
        assert_eq!(free_list.retrieve_free_space(7), None);

        // retrieve more space than available
        free_list.insert_free_space(15, 5);
        assert_eq!(free_list.retrieve_free_space(6), None);

        // retrieve space that matches the exact same space
        free_list.insert_free_space(20, 12);
        assert_eq!(free_list.retrieve_free_space(12), Some(20));
        assert_eq!(free_list.list, vec![FreeSpace {space: 5, cursor: 15}]);

        // pick the smaller space available
        free_list.insert_free_space(10, 300);
        assert_eq!(free_list.retrieve_free_space(5), Some(15));
        assert_eq!(free_list.list, vec![FreeSpace {space: 300, cursor: 10}]);

        // subtract the remaining space when space asked < space available
        assert_eq!(free_list.retrieve_free_space(1), Some(10));
        assert_eq!(free_list.list, vec![FreeSpace {space: 299, cursor: 11}]);
    }

    #[test]
    fn test_compact() {
        let mut free_list = FreeList::new();

        // try to compact empty list
        free_list.compact();

        // insert 1 free space and try to compact
        free_list.insert_free_space(0, 10);
        free_list.compact();
        assert_eq!(free_list.list, vec![FreeSpace {space: 10, cursor: 0}]);

        // insert 1 more free space that is not neighbour and try to compact
        free_list.insert_free_space(30, 11);
        free_list.compact();
        assert_eq!(free_list.list, vec![FreeSpace {space: 10, cursor: 0}, FreeSpace {space: 11, cursor: 30}]);

        // insert one new element that is neighbour of the first free space
        free_list.insert_free_space(10, 5);
        free_list.compact();
        assert_eq!(free_list.list, vec![FreeSpace {space: 11, cursor: 30}, FreeSpace {space: 15, cursor: 0}]);

        // try merge of 5 elements at the same time
        free_list.insert_free_space(15, 10);
        free_list.insert_free_space(25, 4);
        free_list.insert_free_space(29, 1);
        free_list.insert_free_space(41, 2);
        free_list.compact();
        assert_eq!(free_list.list, vec![FreeSpace {space: 43, cursor: 0}]);

        // not merge by one single space
        free_list.insert_free_space(44, 1);
        free_list.compact();
        assert_eq!(free_list.list, vec![FreeSpace {space: 1, cursor: 44}, FreeSpace {space: 43, cursor: 0}]);
    }

    #[test]
    fn test_retrieve_equal_or_bigger_than() {
        let mut free_list = FreeList::new();
        free_list.insert_free_space(0, 10);
        free_list.insert_free_space(15, 5);

        // retrieve free space that is equal to the requested size
        assert_eq!(
            free_list.retrieve_equal_or_bigger_than(&FreeSpace {space: 10, cursor: 0}),
            Some(FreeSpace {space: 10, cursor: 0})
        );
        assert_eq!(free_list.list, vec![FreeSpace {space: 5, cursor: 15}]);

        // retrieve free space that is bigger than the requested size
        assert_eq!(
            free_list.retrieve_equal_or_bigger_than(&FreeSpace {space: 12, cursor: 0}), None
        );
        assert_eq!(free_list.list, vec![FreeSpace {space: 5, cursor: 15}]);

        // retrieve  space that is smaller than available and make sure that the space
        // remaining is reinserted and updated
        assert_eq!(
            free_list.retrieve_equal_or_bigger_than(&FreeSpace {space: 1, cursor: 0}),
            Some(FreeSpace {space: 1, cursor: 15})
        );
        assert_eq!(free_list.list, vec![FreeSpace {space: 4, cursor: 16}])
    }
}
