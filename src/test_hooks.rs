//! Thread-local, one-shot scheduling hooks; never compiled into the library.
use std::{
    cell::RefCell,
    sync::mpsc::{Receiver, SyncSender},
    time::Duration,
};

struct Hook {
    name: &'static str,
    entered: SyncSender<()>,
    resume: Receiver<()>,
}
thread_local! { static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) }; }

pub fn arm(name: &'static str, entered: SyncSender<()>, resume: Receiver<()>) {
    HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Hook {
            name,
            entered,
            resume,
        });
    });
}

pub fn pause(name: &str) {
    HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.as_ref().is_some_and(|hook| hook.name == name) {
            let hook = slot.take().unwrap();
            hook.entered.send(()).unwrap();
            hook.resume
                .recv_timeout(Duration::from_secs(10))
                .expect("test hook not released");
        }
    });
}

thread_local! {
    static FAILURE: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
}
pub fn fail_once(name: &'static str) {
    FAILURE.with(|slot| slot.set(Some(name)));
}
pub fn fail_if_armed(name: &str) -> std::io::Result<()> {
    FAILURE.with(|slot| {
        if slot.get() == Some(name) {
            slot.set(None);
            Err(std::io::Error::other(format!("injected failure: {name}")))
        } else {
            Ok(())
        }
    })
}
