use crate::slot::Slot;

pub struct FreeList {
    list: Vec<Slot>,
    total_free_space: usize,
}

impl FreeList {
    pub fn new() -> Self {
        Self {
            list: Vec::new(),
            total_free_space: 0,
        }
    }

    pub fn new_from_index<K>(mut used_slot_list: Vec<&Slot>) -> Self {
        let mut total_free_space = 0;

        // sort the elements by cursor
        used_slot_list.sort_by(|a, b| a.cursor.cmp(&b.cursor));

        // get the free slots by analyzing the occupied slots
        let mut new_list: Vec<Slot> = vec![];
        let mut previous_slot: &Slot = &Slot{space: 0, cursor: 0};
        for (i, current_slot) in used_slot_list.iter().enumerate() {
            if i == 0 && current_slot.cursor > 0 {
                new_list.push(Slot{space: current_slot.cursor-1, cursor: 0});
                total_free_space += current_slot.cursor-1;
            }

            if i > 0 && current_slot.cursor != (previous_slot.space+previous_slot.cursor+1) {
                new_list.push(Slot{
                    space: current_slot.cursor-previous_slot.cursor,
                    cursor: previous_slot.cursor+previous_slot.space+1
                });
                total_free_space += current_slot.cursor-previous_slot.cursor;
            }

            // save the slot for the next iteration
            previous_slot = current_slot;
        }

        // return updated free list
        return Self{
            list: new_list,
            total_free_space,
        }
    }

    pub fn insert_free_space(&mut self, cursor: usize, space: usize) {
        let value = Slot { cursor, space };
        let pos = match self.list.binary_search(&value) {
            Ok(pos) | Err(pos) => pos,
        };

        self.total_free_space += space;
        self.list.insert(pos, value);
    }

    pub fn retrieve_free_space(&mut self, space: usize) -> Option<usize> {
        let space_cursor = Slot {space: space, cursor: 0};

        if let Some(val) = self.retrieve_equal_or_bigger_than(&space_cursor) {
            self.total_free_space -= val.space;
            return Some(val.cursor)
        }

        return None
    }

    pub fn compact(&mut self) {
        let mut new_list: Vec<Slot> = vec![];
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

    fn retrieve_equal_or_bigger_than(&mut self, expected_amount: &Slot) -> Option<Slot> {
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
            let free_space = Slot {
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
    fn test_new_from_index() {
        // Btree...
        // index.values().collect()
        assert_eq!(1, 2)
    }

    #[test]
    fn test_insert_free_space() {
        // insert one element
        let mut free_list = FreeList::new();
        free_list.insert_free_space(0, 10);
        assert_eq!(free_list.list, vec![Slot {space: 10, cursor: 0}]);

        // insert free space at the beginning
        free_list.insert_free_space(10, 5);
        assert_eq!(
            free_list.list,
            vec![Slot {space: 5, cursor: 10}, Slot {space: 10, cursor: 0}]
        );

        // insert free space at the end
        free_list.insert_free_space(20, 80);
        assert_eq!(
            free_list.list,
            vec![
                Slot {space: 5, cursor: 10},
                Slot {space: 10, cursor: 0},
                Slot {space: 80, cursor: 20},
            ]
        );

        // insert same space but different cursor
        free_list.insert_free_space(30, 8);
        assert_eq!(
            free_list.list,
            vec![
                Slot {space: 5, cursor: 10},
                Slot {space: 8, cursor: 30},
                Slot {space: 10, cursor: 0},
                Slot {space: 80, cursor: 20},
            ]
        );

        // insert cursor already present in the list with different space (should not happen in theory)
        free_list.insert_free_space(0, 11);
        assert_eq!(
            free_list.list,
            vec![
                Slot {space: 5, cursor: 10},
                Slot {space: 8, cursor: 30},
                Slot {space: 10, cursor: 0},
                Slot {space: 11, cursor: 0},
                Slot {space: 80, cursor: 20},
            ]
        );

        // insert same space and same cursor (can't happen in theory)
        free_list.insert_free_space(10, 5);
        assert_eq!(
            free_list.list,
            vec![
                Slot {space: 5, cursor: 10},
                Slot {space: 5, cursor: 10},
                Slot {space: 8, cursor: 30},
                Slot {space: 10, cursor: 0},
                Slot {space: 11, cursor: 0},
                Slot {space: 80, cursor: 20},
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
        assert_eq!(free_list.list, vec![Slot {space: 5, cursor: 15}]);

        // pick the smaller space available
        free_list.insert_free_space(10, 300);
        assert_eq!(free_list.retrieve_free_space(5), Some(15));
        assert_eq!(free_list.list, vec![Slot {space: 300, cursor: 10}]);

        // subtract the remaining space when space asked < space available
        assert_eq!(free_list.retrieve_free_space(1), Some(10));
        assert_eq!(free_list.list, vec![Slot {space: 299, cursor: 11}]);
    }

    #[test]
    fn test_compact() {
        let mut free_list = FreeList::new();

        // try to compact empty list
        free_list.compact();

        // insert 1 free space and try to compact
        free_list.insert_free_space(0, 10);
        free_list.compact();
        assert_eq!(free_list.list, vec![Slot {space: 10, cursor: 0}]);

        // insert 1 more free space that is not neighbour and try to compact
        free_list.insert_free_space(30, 11);
        free_list.compact();
        assert_eq!(free_list.list, vec![Slot {space: 10, cursor: 0}, Slot {space: 11, cursor: 30}]);

        // insert one new element that is neighbour of the first free space
        free_list.insert_free_space(10, 5);
        free_list.compact();
        assert_eq!(free_list.list, vec![Slot {space: 11, cursor: 30}, Slot {space: 15, cursor: 0}]);

        // try merge of 5 elements at the same time
        free_list.insert_free_space(15, 10);
        free_list.insert_free_space(25, 4);
        free_list.insert_free_space(29, 1);
        free_list.insert_free_space(41, 2);
        free_list.compact();
        assert_eq!(free_list.list, vec![Slot {space: 43, cursor: 0}]);

        // not merge by one single space
        free_list.insert_free_space(44, 1);
        free_list.compact();
        assert_eq!(free_list.list, vec![Slot {space: 1, cursor: 44}, Slot {space: 43, cursor: 0}]);
    }

    #[test]
    fn test_retrieve_equal_or_bigger_than() {
        let mut free_list = FreeList::new();
        free_list.insert_free_space(0, 10);
        free_list.insert_free_space(15, 5);

        // retrieve free space that is equal to the requested size
        assert_eq!(
            free_list.retrieve_equal_or_bigger_than(&Slot {space: 10, cursor: 0}),
            Some(Slot {space: 10, cursor: 0})
        );
        assert_eq!(free_list.list, vec![Slot {space: 5, cursor: 15}]);

        // retrieve free space that is bigger than the requested size
        assert_eq!(
            free_list.retrieve_equal_or_bigger_than(&Slot {space: 12, cursor: 0}), None
        );
        assert_eq!(free_list.list, vec![Slot {space: 5, cursor: 15}]);

        // retrieve  space that is smaller than available and make sure that the space
        // remaining is reinserted and updated
        assert_eq!(
            free_list.retrieve_equal_or_bigger_than(&Slot {space: 1, cursor: 0}),
            Some(Slot {space: 1, cursor: 15})
        );
        assert_eq!(free_list.list, vec![Slot {space: 4, cursor: 16}])
    }
}
