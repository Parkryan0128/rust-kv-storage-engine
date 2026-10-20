use std::io;
#[inline]
pub(crate) fn hit(_name: &str) -> io::Result<()> {
    #[cfg(test)]
    crate::test_hooks::fail_if_armed(_name)?;
    #[cfg(feature = "fault-injection")]
    if std::env::var("KV_FAILPOINT").as_deref() == Ok(_name) {
        if std::env::var("KV_FAIL_ACTION").as_deref() == Ok("error") {
            return Err(io::Error::other(format!("injected failure: {_name}")));
        }
        std::process::exit(86);
    }
    Ok(())
}
