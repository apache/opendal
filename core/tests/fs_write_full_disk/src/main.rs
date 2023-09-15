use std::process::Command;
use std::io::Result;


#[test]
fn test_fs_write_full_disk() -> Result<()> {
    // 执行命令
    Command::new("fallocate")
        .args(&["-l", "512K", "disk.img"])
        .output()?;
    Command::new("mkfs")
        .arg("disk.img")
        .output()?;
    Command::new("mkdir")
        .arg("./td")
        .output()?;
    Command::new("sudo")
        .args(&["mount", "-o", "loop", "disk.img", "./td"])
        .output()?;
    Command::new("chmod")
        .arg("a+wr")
        .arg("./td")
        .output()?;

    // 可以添加更多断言
    // ...

    Ok(())
}
