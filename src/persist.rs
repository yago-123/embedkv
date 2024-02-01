
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use crate::fileheader::FileHeader;
use crate::freelist::FreeList;
use serde::{Serialize, Deserialize};

pub struct Persister {
    freelist: FreeList,  
    header: FileHeader,
    last_cursor: usize,
}

impl Persister {
    pub fn new(datastore: String, storage_limit: usize) -> Result<Self, std::io::Error> {
        FileHeader::new(Some(datastore))
            .map(|fh| Self { freelist: FreeList::new(), header: fh, last_cursor: 0 })
    }

    pub fn insert_kv<'a, K>(&mut self, key: K, value: &Vec<u8>) -> Result<usize, std::io::Error>
    where K: Serialize + Deserialize<'a> {
        let mut cursor: usize;

        // try to retrieve free space, otherwise, add in the last cursor
        match self.freelist.retrieve_free_space(value.len()) {
            Some(empty_space_cursor) => cursor = empty_space_cursor,
            None => {
                cursor = self.last_cursor;
                self.last_cursor = self.last_cursor + value.len();
            }
        }

        if let Err(error) = self.insert_value_in_position(&value, cursor) {
            // make sure to free the memory to prevent leaks
            if cursor == self.last_cursor - value.len() {
                self.last_cursor = cursor - value.len()
            }
            return Err(error)
        }

        // todo(): serialize and store the key

        return Ok(cursor);
    }

    pub fn retrieve_kv<K>(&mut self, key: K) -> Result<Vec<u8>, std::io::Error> {
        return Ok(vec![]);
    }

    fn insert_value(&mut self, data: &Vec<u8>, cursor: usize) -> Result<(), std::io::Error> {
        self.header.file.seek(SeekFrom::Start(cursor as u64))?;
        self.header.file.write_all(data.as_ref())?;

        Ok(())
    }

    fn retrieve_value(&mut self, cursor: usize, space: usize) -> Result<Vec<u8>, std::io::Error> {
        // todo(buffer): use a fixed buffer instead of a vec
        let mut buffer = Vec::with_capacity(space);

        // todo: handle the error and returns
        self.header.file.seek(SeekFrom::Start(cursor as u64));
        self.header.file.read_at(&mut buffer.as_mut_slice(), cursor as u64)?;

        return Ok(buffer)
    }
}