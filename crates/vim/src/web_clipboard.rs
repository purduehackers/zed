//! Resume clipboard-dependent commands only after a fresh browser read.

use crate::{
    Vim, VimSettings,
    helix::HelixPaste,
    motion::Motion,
    normal::paste::Paste,
    object::Object,
    state::{Mode, Operator, ReplayableAction, VimGlobals},
};
use editor::Anchor;
use gpui::{Action, App, Context, EntityId, Window};
use language::Selection;
use settings::{Settings, UseSystemClipboard};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Clone)]
struct Completion(Arc<AtomicBool>);
impl PartialEq for Completion {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone, PartialEq)]
pub(crate) enum Operation {
    Paste(Paste),
    HelixPaste(HelixPaste),
    InsertRegister(Arc<str>),
    Motion(Motion),
    Object(Object, bool),
}

#[derive(Clone, PartialEq)]
struct Snapshot {
    editor: EntityId,
    mode: Mode,
    operators: Vec<Operator>,
    register: Option<char>,
    counts: (Option<usize>, Option<usize>, bool),
    selections: Arc<[Selection<Anchor>]>,
    edits: usize,
}

#[derive(Clone, PartialEq, Action)]
#[action(namespace = vim, no_json, no_register)]
pub struct WebClipboardAction {
    operation: Operation,
    expected: Snapshot,
    was_recording: bool,
    finished: Completion,
}

impl WebClipboardAction {
    pub fn finish(&self, cx: &mut App) {
        if !self.finished.0.swap(true, Ordering::Relaxed) {
            let globals = Vim::globals(cx);
            globals.web_clipboard_pending = globals.web_clipboard_pending.saturating_sub(1);
        }
    }
}

impl Vim {
    fn clipboard_snapshot(&self, cx: &mut App) -> Option<Snapshot> {
        let editor = self.editor()?;
        let (selections, edits) = editor.update(cx, |view, cx| {
            let display = view.display_snapshot(cx);
            (
                view.selections.all_anchors(&display),
                display.buffer_snapshot().edit_count(),
            )
        });
        let globals = cx.global::<VimGlobals>();
        Some(Snapshot {
            editor: editor.entity_id(),
            mode: self.mode,
            operators: self.operator_stack.clone(),
            register: self.selected_register,
            counts: (globals.pre_count, globals.post_count, globals.forced_motion),
            selections,
            edits,
        })
    }

    pub(crate) fn defer_web_clipboard(
        &self,
        operation: Operation,
        register: Option<char>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let system = match register {
            Some('+' | '*') => true,
            None | Some('"') => {
                VimSettings::get_global(cx).use_system_clipboard != UseSystemClipboard::Never
            }
            _ => false,
        };
        if !system || cx.read_from_clipboard().is_some() {
            return false;
        }
        let Some(expected) = self.clipboard_snapshot(cx) else {
            return false;
        };
        Vim::globals(cx).web_clipboard_pending += 1;
        window.dispatch_action(
            Box::new(WebClipboardAction {
                operation,
                expected,
                was_recording: cx.global::<VimGlobals>().dot_recording,
                finished: Completion(Arc::new(AtomicBool::new(false))),
            }),
            cx,
        );
        true
    }

    pub(crate) fn complete_web_clipboard(
        &mut self,
        action: &WebClipboardAction,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        action.finish(cx);
        if self.clipboard_snapshot(cx).as_ref() != Some(&action.expected) {
            return; // Focus, text, cursor or pending command changed during permission UI.
        }
        match &action.operation {
            Operation::Paste(action) => self.paste(action, window, cx),
            Operation::HelixPaste(action) => self.helix_paste(action, window, cx),
            Operation::InsertRegister(text) => self.input_ignored(text.clone(), window, cx),
            Operation::Motion(motion) => self.motion(motion.clone(), window, cx),
            Operation::Object(object, opening) => self.object_impl(*object, *opening, window, cx),
        }
        // The physical keystroke was already observed before the asynchronous read.
        // Finish dot-recording now, rather than accidentally recording the next key.
        let globals = Vim::globals(cx);
        if globals.dot_recording && globals.stop_recording_after_next_action {
            if !action.was_recording {
                let paste = match &action.operation {
                    Operation::Paste(action) => Some(action.boxed_clone()),
                    Operation::HelixPaste(action) => Some(action.boxed_clone()),
                    _ => None,
                };
                if let Some(paste) = paste {
                    globals
                        .recording_actions
                        .push(ReplayableAction::Action(paste));
                }
            }
            globals.recorded_actions = std::mem::take(&mut globals.recording_actions);
            globals.recorded_count = globals.recording_count.take();
            globals.recorded_register_for_dot = globals.recording_register_for_dot.take();
            globals.dot_recording = false;
            globals.stop_recording_after_next_action = false;
        }
        if self.exit_temporary_mode {
            self.exit_temporary_mode = false;
            self.switch_mode(Mode::Insert, false, window, cx);
        }
    }
}
