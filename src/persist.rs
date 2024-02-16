use std::collections::BTreeMap;
use std::io::{Seek, SeekFrom, Write, Read};
use std::os::unix::fs::FileExt;
use crate::fileheader::FileHeader;
use crate::freelist::FreeList;
use crate::slot::Slot;
use std::fs::File;
use serde::{Serialize, Deserialize};
use crate::persist::KVError::{KeyAlreadyExist, KeyDoesNotExist};

#[derive(Debug, PartialEq)]
enum KVError {
    KeyDoesNotExist,
    KeyAlreadyExist,
    IOError(String),
}

pub struct Persister<K> {
    freelist: FreeList,  
    header: FileHeader,
    index: BTreeMap<K, Slot>, // todo(): unify SlotInstance with a more common name
    last_cursor: usize,
}

impl<K> Persister<K> where K: Ord + Clone {
    pub fn new(datastore: String, storage_limit: usize) -> Result<Self, KVError> {
        FileHeader::new(Some(datastore))
            .map(|fh| Self { freelist: FreeList::new(), header: fh, index: BTreeMap::new(), last_cursor: 0 })
            .map_err(|io_error| KVError::IOError(io_error.to_string()))
    }

    pub fn insert_kv<'a>(&mut self, key: &K, value: &Vec<u8>) -> Result<(), KVError>
    where K: Serialize + Deserialize<'a> {
        let mut cursor: usize = 0;

        if self.index.contains_key(&key) {
            return Err(KeyAlreadyExist)
        }

        if value.len() > 0 {
            // try to retrieve free space, otherwise, add in the last cursor
            match self.freelist.retrieve_free_space(value.len()) {
                Some(empty_space_cursor) => cursor = empty_space_cursor,
                None => {
                    cursor = self.last_cursor;
                    self.last_cursor = self.last_cursor + value.len();
                }
            }

            if let Err(error) = self.persist_value(&value, cursor) {
                // make sure to free the memory to prevent leaks
                if cursor == self.last_cursor - value.len() {
                    self.last_cursor = cursor - value.len()
                }
                return Err(error)
            }
        }

        // todo(): serialize and store the key in file
        self.persist_key();

        // insert key in index
        if self.index.insert(key.clone(), Slot {cursor, space: value.len()}).is_none() {
            // todo(): return error and undo things (insert the slot as free space)
        }

        return Ok(());
    }

    pub fn get_value(&mut self, key: &K) -> Result<Vec<u8>, KVError> {
        match self.index.get(key) {
            Some(val) => {
                return self.retrieve_value(val.cursor, val.space);
            },
            None => {
                return Err(KeyDoesNotExist);
            }
        }
    }

    pub fn update_kv(&mut self, key: K, value: &Vec<u8>) -> Result<(), KVError> {
        let mut slot = Slot{space: 0, cursor: 0};
        match self.index.get(&key) {
            Some(val) => {
                slot = val.clone();
            },
            None => return Err(KeyDoesNotExist),
        }

        // free previous data and claim more space
        if value.len() > slot.space {
            self.freelist.insert_free_space(slot.cursor, slot.space);
            match self.freelist.retrieve_free_space(slot.space) {
                Some(val) => {
                    slot.cursor = val;
                },
                None => {
                    slot.cursor = self.last_cursor;
                    self.last_cursor = self.last_cursor + value.len();
                },
            }
        }

        // downsize the leftover space if the space is smaller
        if value.len() < slot.space {
            self.freelist.insert_free_space(slot.cursor+value.len(), slot.space - value.len());
        }

        // update slot space required
        slot.space = value.len();

        // persist the value
        self.persist_value(value, slot.cursor);

        // todo(): serialize the new key data
        self.persist_key();

        // update the index
        self.index.insert(key, Slot{cursor: slot.cursor, space: slot.space});

        return Ok(())
    }

    pub fn delete_kv(&mut self, key: &K) -> Result<(), KVError> {
        // check if key exists and insert freed space
        match self.index.get(key) {
            Some(val) => self.freelist.insert_free_space(val.cursor, val.space),
            None => return Err(KeyDoesNotExist),
        }

        // todo(): remove serialized key from file
        // insert key space into file
        self.delete_key();

        // remove key from index
        match self.index.remove(key) {
            Some(_) => Ok(()),
            None => Err(KeyDoesNotExist), // should never happen
        }
    }

    fn persist_value(&mut self, data: &Vec<u8>, cursor: usize) -> Result<(), KVError> {
        self.header.db_file.seek(SeekFrom::Start(cursor as u64))
            .map_err(|io_error| KVError::IOError(io_error.to_string()))?;
        self.header.db_file.write_all(data.as_ref())
            .map_err(|io_error| KVError::IOError(io_error.to_string()))?;

        Ok(())
    }

    fn retrieve_value(&mut self, cursor: usize, space: usize) -> Result<Vec<u8>, KVError> {
        // todo(buffer): use a fixed buffer instead of a vec
        let mut buffer = Vec::with_capacity(space);

        // todo: handle the error and returns
        self.header.db_file.seek(SeekFrom::Start(cursor as u64));
        self.header.db_file.read_at(&mut buffer.as_mut_slice(), cursor as u64)
            .map_err(|io_error| KVError::IOError(io_error.to_string()))?;

        return Ok(buffer)
    }

    fn persist_key(&mut self) -> Result<(), KVError> {
        return Ok(());
    }

    fn delete_key(&mut self) -> Result<(), KVError> {
        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use super::*;

    fn new_mock_persister() -> Persister<String> {
        Persister {
            freelist: FreeList::new(),
            header: FileHeader {
                db_file: tempfile::tempfile().unwrap(),
                index_file: tempfile::tempfile().unwrap(),
            },
            index: BTreeMap::new(),
            last_cursor: 0,
        }
    }

    #[test]
    fn test_insert_kv_empty_values() {
        let mut persister = new_mock_persister();

        assert_eq!(Ok(()), persister.insert_kv(&"empty_value".to_string(), &vec![]));
        assert_eq!(
            Slot{cursor: 0, space: 0},
            persister.index.get(&"empty_value".to_string()).unwrap().clone()
        );
        assert_eq!(0, persister.last_cursor);
    }

    #[test]
    fn test_insert_kv_two_times_same_key() {
        let mut persister = new_mock_persister();

        assert_eq!(Ok(()), persister.insert_kv(&"key_duplicated".to_string(), &vec![]));
        assert_eq!(KeyAlreadyExist, persister.insert_kv(&"key_duplicated".to_string(), &vec![]).unwrap_err());
        assert_eq!(0, persister.last_cursor);
    }

    #[test]
    fn test_insert_kv_multiple_kvs() {
        let mut persister = new_mock_persister();
        let keys: Vec<String> = vec![
            "key_1".to_string(),
            "key_2".to_string(),
            "key_3".to_string(),
            "key_4".to_string(),
            "key_5".to_string(),
        ];

        let values: Vec<Vec<u8>> = vec![
            vec![b'a', b'b', b'c'],
            vec![b'd', b'e', b'f', b'g'],
            vec![b'h', b'i', b'j', b'k', b'l'],
            vec![b'm', b'n', b'o', b'p'],
            vec![b'q', b'r', b's', b't', b'u', b'v'],
        ];

        let slots: Vec<Slot> = vec![
            Slot { space: 3, cursor: 0 },
            Slot { space: 4, cursor: 3 },
            Slot { space: 5, cursor: 7 },
            Slot { space: 4, cursor: 12 },
            Slot { space: 6, cursor: 16 },
        ];

        // insert multiple non empty values and make sure that cursor is incremented
        let mut expected_cursor = 0;
        for kv in keys.iter().zip(values.iter()) {
            persister.insert_kv(kv.0, kv.1).unwrap();
            assert_eq!(expected_cursor, persister.last_cursor);

            expected_cursor += kv.1.len();
        }

        // make sure that all keys can be retrieved with the corresponding slot
        let mut iteration = 0;
        for kv in keys.iter().zip(values.iter()) {
            assert_eq!(
                slots[iteration],
                persister.index.get(kv.0).unwrap().clone()
            );

            iteration += 1;
        }

        // check that the resulting file is the same
        persister.header.db_file.flush().unwrap();
        assert_eq!(1, 2);
        // assert_slots_eq(
        //      open_file("tests/data/insert_kv-01.dat"),
        //      persister.header.db_file,
        //      &vec![Slot{cursor: 0, space: 0}]
        // )
    }

    #[test]
    fn test_insert_kv_check_free_spots() {
        let mut persister = new_mock_persister();

        // create a free spot in the middle of two keys with size 2 and test whether we
        // make use of the free space generated
        persister.insert_kv(&"key_1".to_string(), &vec![b'a', b'b', b'c']);
        persister.insert_kv(&"key_2".to_string(), &vec![b'd', b'e']);
        persister.insert_kv(&"key_3".to_string(), &vec![b'f', b'g', b'h']);

        // delete the middle kv
        persister.delete_kv(&"key_2".to_string()).unwrap();

        persister.insert_kv(&"key_4".to_string(), &vec![b'i', b'j', b'k']);
        assert_eq!(8, persister.index.get(&"key_4".to_string()).unwrap().cursor);
        assert_eq!(3, persister.index.get(&"key_4".to_string()).unwrap().space);

        persister.insert_kv(&"key_5".to_string(), &vec![b'l']);
        assert_eq!(3, persister.index.get(&"key_5".to_string()).unwrap().cursor);
        assert_eq!(1, persister.index.get(&"key_5".to_string()).unwrap().space);
    }

    #[test]
    fn test_get_value() {
        assert_eq!(1, 2)
    }

    #[test]
    fn test_update_value() {
        assert_eq!(1, 2)
    }

    #[test]
    fn delete_kv() {
        assert_eq!(1, 2)
    }

    fn assert_slots_eq(mut file_exp: File, mut file_obt: File, slots: &Vec<Slot>) {
        let highest_cursor = slots.iter().map(|slot| slot.cursor + slot.space).max().unwrap_or(0);

        assert_ne!(0, highest_cursor);
        assert_ne!(0, slots.len());

        let mut read_exp = vec![0; highest_cursor];
        file_exp.seek(SeekFrom::Start(0)).unwrap();
        file_exp.read_exact(&mut read_exp).unwrap();

        let mut read_obt = vec![0; highest_cursor];
        file_obt.seek(SeekFrom::Start(0)).unwrap();
        file_obt.read_exact(&mut read_obt).unwrap();

        // only compare the slots, files may contain junk in unwritten parts
        for slot in slots.iter() {
            assert_eq!(
                read_exp[slot.cursor..slot.cursor+slot.space],
                read_obt[slot.cursor..slot.cursor+slot.space],
            );
        }
    }

    fn open_file(name: &str) -> File {
        OpenOptions::new()
            .read(true)
            .open(name).unwrap()
    }
}
