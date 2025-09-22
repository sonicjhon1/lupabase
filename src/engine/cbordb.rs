use super::memorydb::MemoryDB;
use crate::{Deserialize, Error, Result, database::*};
use std::{
    fs::{self, create_dir_all},
    path::{Path, PathBuf},
};
use tracing::{info, warn};

#[derive(Clone, Debug)]
pub struct CborDB {
    db_dir: PathBuf,
}

impl Database for CborDB {
    const NAME: &'static str = "CborDB";

    fn new(dir: impl AsRef<Path>) -> Self {
        let dir = dir.as_ref();

        create_dir_all(dir)
            .map_err(|e| Error::IOCreateDirFailure {
                path: dir.display().to_string(),
                reason: e,
            })
            .expect("CborDB directories creation should succeed.");

        Self { db_dir: dir.into() }
    }
    fn dir(&self) -> PathBuf {
        self.db_dir.clone()
    }
    fn file_path(&self, file_name: impl AsRef<Path>) -> PathBuf {
        self.dir().join(file_name).with_added_extension("Cbordb")
    }
}

impl DatabaseOps for CborDB {}

impl DatabaseIO for CborDB {
    fn try_initialize_file<T: DatabaseRecord>(
        &self,
        default_records: impl AsRef<[T]>,
    ) -> Result<()> {
        let file_path = &self.file_path(T::PARTITION);

        match self.try_read_file::<T, Vec<T>>() {
            Ok(_) => {}
            Err(Error::DBNotFound { file_path }) => {
                warn!(
                    "Couldn't find [{}]. Trying to populate {}.",
                    file_path.display(),
                    Self::NAME
                );

                self.try_write_file::<T>(default_records.as_ref())?;
            }
            Err(e) => return Err(e),
        };

        info!("Found [{}].", file_path.display());
        return Ok(());
    }
    fn try_write_file<T: DatabaseRecord>(&self, data: impl serde::Serialize) -> Result<()> {
        let file_path = &self.file_path(T::PARTITION);
        let serialized =
            minicbor_serde::to_vec(&data).map_err(|e| Error::SerializationFailure(Box::new(e)))?;

        if let Some(parent) = file_path.parent() {
            create_dir_all(parent).map_err(|e| Error::IOCreateDirFailure {
                path: parent.display().to_string(),
                reason: e,
            })?;
        }

        if file_path.is_dir() {
            return Err(Error::DBCorrupt {
                file_path: file_path.to_path_buf(),
                reason: std::io::ErrorKind::IsADirectory {}.to_string(),
            });
        }

        return fs::write(file_path, serialized).map_err(|e| Error::IOWriteFailure {
            path: file_path.display().to_string(),
            reason: e,
        });
    }
    fn try_read_file<T: DatabaseRecord, O: for<'a> Deserialize<'a>>(&self) -> Result<O> {
        let file_path = &self.file_path(T::PARTITION);
        let file_data = fs::read(file_path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => Error::DBNotFound {
                file_path: file_path.to_path_buf(),
            },
            _ => Error::DBCorrupt {
                file_path: file_path.to_path_buf(),
                reason: e.to_string(),
            },
        })?;

        minicbor_serde::from_slice(&file_data).map_err(|e| {
            let backup_path = file_path
                .with_extension(format!("-FAILED-{}.bak", &chrono::Local::now().timestamp()));

            warn!(
                "Failed deserialize file at [{}], creating a new backup at [{}], caused by: [{e}]",
                file_path.display(),
                backup_path.display()
            );

            if let Err(e) = fs::copy(file_path, &backup_path) {
                return Error::IOCopyFailure {
                    path_from: file_path.display().to_string(),
                    path_destination: backup_path.display().to_string(),
                    reason: e,
                };
            };

            info!("Backup created successfully at [{}]", backup_path.display());

            return Error::DBCorrupt {
                file_path: file_path.to_path_buf(),
                reason: Error::DeserializationFailure(Box::new(e)).to_string(),
            };
        })
    }
}

impl DatabaseTransaction for CborDB {
    type TransactionDB = CborDBTransaction;

    fn try_rollback<T: DatabaseRecord>(&self, transaction: &Self::TransactionDB) -> Result<()> {
        let records_before = transaction.records_before.get_all::<T>()?;
        return self.try_write_file::<T>(records_before);
    }
}

#[derive(Clone, Debug)]
pub struct CborDBTransaction {
    dir: PathBuf,
    records_before: MemoryDB,
    records_after: MemoryDB,
}

impl Database for CborDBTransaction {
    const NAME: &'static str = "CborDB-Transaction";

    fn new(dir: impl AsRef<Path>) -> Self {
        let memory_db = MemoryDB::new(&dir);
        return Self {
            dir: dir.as_ref().to_path_buf(),
            records_before: memory_db.clone(),
            records_after: memory_db,
        };
    }

    fn dir(&self) -> PathBuf {
        self.dir.clone()
    }

    fn file_path(&self, file_name: impl AsRef<Path>) -> PathBuf {
        self.dir.join(file_name)
    }
}

impl DatabaseOps for CborDBTransaction {}

impl DatabaseIO for CborDBTransaction {
    fn try_initialize_file<T: DatabaseRecord>(
        &self,
        default_records: impl AsRef<[T]>,
    ) -> Result<()> {
        match self.try_read_file::<T, Vec<T>>() {
            Ok(_) => {}
            Err(Error::DBNotFound { file_path }) => {
                warn!(
                    "Couldn't find [{}]. Trying to populate {}.",
                    file_path.display(),
                    Self::NAME
                );

                self.records_after
                    .try_initialize_file::<T>(default_records.as_ref())?;
            }
            Err(e) => return Err(e),
        };

        info!("Found [{}].", self.file_path(T::PARTITION).display());
        return Ok(());
    }

    fn try_write_file<T: DatabaseRecord>(&self, data: impl serde::Serialize) -> Result<()> {
        self.records_after.try_write_file::<T>(data)
    }

    fn try_read_file<T: DatabaseRecord, O: for<'a> Deserialize<'a>>(&self) -> Result<O> {
        self.records_after.try_read_file::<T, O>()
    }
}
