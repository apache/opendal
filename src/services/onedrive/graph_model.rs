use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct DriveItem {
    createdDateTime: String,
    cTag: String,
    eTag: String,
    id: String,
    lastModifiedDateTime: String,
    name: String,
    size: u64,
    webUrl: String,
    reactions: Option<Reactions>,
    createdBy: Option<CreatedBy>,
    lastModifiedBy: Option<LastModifiedBy>,
    parentReference: ParentReference,
    fileSystemInfo: FileSystemInfo,
    folder: Option<Folder>,
    specialFolder: Option<SpecialFolder>,
}

#[derive(Debug, Deserialize)]
struct Reactions {
    commentCount: u64,
}

#[derive(Debug, Deserialize)]
struct CreatedBy {
    user: Option<User>,
    application: Option<Application>,
}

#[derive(Debug, Deserialize)]
struct LastModifiedBy {
    user: Option<User>,
    application: Option<Application>,
}

#[derive(Debug, Deserialize)]
struct User {
    displayName: String,
    id: String,
}

#[derive(Debug, Deserialize)]
struct Application {
    displayName: String,
    id: String,
}

#[derive(Debug, Deserialize)]
struct ParentReference {
    driveId: String,
    driveType: String,
    id: String,
    path: String,
}

#[derive(Debug, Deserialize)]
struct FileSystemInfo {
    createdDateTime: String,
    lastModifiedDateTime: String,
}

#[derive(Debug, Deserialize)]
struct Folder {
    childCount: u64,
    view: View,
}

#[derive(Debug, Deserialize)]
struct View {
    viewType: String,
    sortBy: String,
    sortOrder: String,
}

#[derive(Debug, Deserialize)]
struct SpecialFolder {
    name: String,
}

#[derive(Debug, Deserialize)]
struct DriveItemList {
    value: Vec<DriveItem>,
    count: u64,
    context: String,
}

