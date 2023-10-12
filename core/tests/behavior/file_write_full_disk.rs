#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_file_write_on_full_disk() {
        let data = [96u8; 1024 * 512]; // The writable was smaller than 512KB
        op.write("/test", data.to_vec()).await.unwrap();
    }
}
