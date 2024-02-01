use std::fs::{File, OpenOptions};
use uuid::Uuid;

pub struct FileHeader {
    pub(crate) file: File,
    page_count: u32,
}

impl FileHeader {
    pub fn new(datastore: Option<String>) -> Result<Self, std::io::Error> {
        let mut name = Uuid::new_v4().to_string();
        if let Some(ds_name) = datastore {
            name = ds_name
        }

        let mut file_handler = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true) // todo(): remove this one
            .open(name);

        match file_handler {
            Ok(file) => Ok(Self {
                page_count: 0,
                file: file,
            }),
            Err(error) => Err(error),
        }
    }
}