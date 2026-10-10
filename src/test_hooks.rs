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
