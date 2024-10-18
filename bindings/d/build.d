module build;
import std.stdio: writeln;
import std.path: buildPath;
import std.process: spawnShell, wait;
import std.exception;
import std.file: copy, mkdir, exists;
import std.conv: to;

version (Windows)
    enum staticlib = "opendal_c.lib";
else
    enum staticlib = "libopendal_c.a";

void main (string[] args)
{
    bool isRelease = args.length > 1 && args[1] == "release";
    string buildType = isRelease ? "release" : "debug";

    // Run cargo build
    auto cargoCmd = "cargo build --manifest-path " ~ buildPath(
        "..", "c", "Cargo.toml") ~ (isRelease ? " --release" : "");

    auto status = wait(spawnShell(cargoCmd));
    if (status != 0)
    {
        throw new Exception("Cargo build failed with status: " ~ status.to!string);
    }
    else
    {
        writeln("Cargo build completed successfully");
    }

    // Get opendal.h
    copy(buildPath("..", "c", "include", "opendal.h"), buildPath("source", "opendal", "opendal.h"));

    // Get libopendal_c.a
    auto libPath = buildPath("..", "c", "target", buildType, staticlib);
    writeln("Copying ", libPath, " to ", buildPath("lib", staticlib));
    if (!exists("lib"))
        mkdir ("lib");
    copy(libPath, buildPath("lib", staticlib));
}
