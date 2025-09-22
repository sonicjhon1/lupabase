use crate::{
    Deserialize, Error, Result, Serialize, database::Database, record::*,
    record_utils::DatabaseRecordsUtils,
};
use std::{
    fs::{self, create_dir_all},
    path::Path,
};
use tracing::{info, warn};

pub fn check_is_all_new_records<R: DatabaseRecord>(
    current_records: &[R],
    new_records: &[R],
    path: impl AsRef<Path>,
) -> Result<()> {
    let duplicates = &current_records.find_intersecting_uniques_from(new_records);
    if !duplicates.is_empty() {
        return Err(Error::DBOperationFailure {
            path: path.as_ref().display().to_string(),
            reason: format!(
                "Found duplicate Unique value(s) in record(s) when inserting: [{duplicates:#?}].",
            ),
        });
    };

    Ok(())
}

pub fn check_is_all_existing_records<R: DatabaseRecord>(
    current_records: &[R],
    new_records: &[R],
    path: impl AsRef<Path>,
) -> Result<()> {
    let non_matching = &current_records.find_non_intersecting_uniques_from(new_records);
    if !non_matching.is_empty() {
        return Err(Error::DBOperationFailure {
            path: path.as_ref().display().to_string(),
            reason: format!(
                "Found non-matching Unique value(s) in record(s) when updating: [{non_matching:?}].",
            ),
        });
    };

    Ok(())
}

pub fn try_populate_storage<D: Database, O: Serialize + for<'a> Deserialize<'a>>(
    database: &D,
    default_data: O,
    path: impl AsRef<Path>,
) -> Result<()> {
    match database.try_read_storage::<O>(&path) {
        Ok(_) => {}
        Err(Error::DBNotFound { file_path }) => {
            warn!(
                "Couldn't find [{}]. Trying to populate {}.",
                file_path.display(),
                D::NAME
            );

            database.try_write_storage(default_data, &path)?;
        }
        Err(e) => return Err(e),
    };

    info!("Found [{}].", path.as_ref().display());
    return Ok(());
}

pub fn backup_failed_parse<D: Database>(
    database: &D,
    path: impl AsRef<Path>,
    error: impl std::error::Error + 'static,
) -> Error {
    let path = path.as_ref();

    warn!(
        "Failed deserialize file at [{}], creating a new backup, caused by: [{error}]",
        path.display(),
    );

    return match database.try_backup_storage(path, "FAILED_PARSING") {
        Ok(backup_path) => {
            info!("Backup created successfully at [{}]", backup_path.display());

            return Error::DBCorrupt {
                file_path: path.to_path_buf(),
                reason: Error::DeserializationFailure(Box::new(error)).to_string(),
            };
        }
        Err(e) => e,
    };
}

pub fn try_write_file(serialized_bytes: &[u8], path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();

    if let Some(parent) = path.parent() {
        create_dir_all(parent).map_err(|e| Error::IOCreateDirFailure {
            path: parent.display().to_string(),
            reason: e,
        })?;
    }

    if path.is_dir() {
        return Err(Error::DBCorrupt {
            file_path: path.to_path_buf(),
            reason: std::io::ErrorKind::IsADirectory {}.to_string(),
        });
    }

    return fs::write(path, serialized_bytes).map_err(|e| Error::IOWriteFailure {
        path: path.display().to_string(),
        reason: e,
    });
}

pub fn try_read_file(path: impl AsRef<Path>) -> Result<Vec<u8>> {
    let path = path.as_ref();

    return fs::read(path).map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => Error::DBNotFound {
            file_path: path.to_path_buf(),
        },
        _ => Error::DBCorrupt {
            file_path: path.to_path_buf(),
            reason: e.to_string(),
        },
    });
}
