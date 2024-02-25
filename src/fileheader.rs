use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind};
use uuid::Uuid;

pub struct FileHeader {
    pub(crate) db_file: File,
    pub(crate) index_file: File,
}

impl FileHeader {
    pub fn new(datastore_name: Option<String>) -> Result<Self, std::io::Error> {
        let mut name = Uuid::new_v4().to_string();
        if let Some(ds_name) = datastore_name {
            name = ds_name
        }

        let db_file_handler = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true) // todo(): remove this one
            .open(&name);

        let index_file_handler = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true) // todo(): remove this one
            .open(format!("{}_{}", "index".to_string(), &name));

        match (db_file_handler, index_file_handler) {
            (Ok(db_file), Ok(index_file)) => Ok(Self {
                db_file,
                index_file,
            }),
            (_, _) => Err(Error::new(ErrorKind::Other, "The key introduced was not registered")),
        }
    }
}