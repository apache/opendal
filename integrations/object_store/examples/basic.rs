use bytes::Bytes;
#[cfg(feature = "services-s3")]
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectStore;
use object_store_opendal::OpendalStore;

#[cfg(feature = "services-s3")]
#[tokio::main]
async fn main() {
    let builder = AmazonS3Builder::default()
        .with_bucket_name("my_bucket")
        .with_region("my_region")
        .with_endpoint("my_endpoint")
        .with_access_key_id("my_access_key")
        .with_secret_access_key("my_secret_access_key");
    let s3_store = OpendalStore::new_amazon_s3(builder).unwrap();

    let path = ObjectStorePath::from("data/nested/test.txt");
    let bytes = Bytes::from_static(b"hello, world! I am nested.");

    s3_store.put(&path, bytes.clone().into()).await.unwrap();

    let content = s3_store.get(&path).await.unwrap().bytes().await.unwrap();

    assert_eq!(content, bytes);
}

#[cfg(not(feature = "services-s3"))]
fn main() {
    println!("The 'services-s3' feature is not enabled.");
}
