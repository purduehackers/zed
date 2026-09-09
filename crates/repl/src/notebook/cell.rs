use std::sync::Arc;
use std::time::Duration;
#[cfg(not(target_family = "wasm"))]
use std::time::Instant;
#[cfg(target_family = "wasm")]
use web_time::Instant;

use editor::{Editor, EditorMode, MultiBuffer, SizingBehavior};
use futures::future::Shared;
use gpui::{
    App, Entity, EventEmitter, Focusable, Hsla, InteractiveElement, RetainAllImageCache,
    StatefulInteractiveElement, Task, prelude::*,
};
use jupyter_protocol::{JupyterMessage, JupyterMessageContent};
use language::{Buffer, Language, LanguageRegistry};
use markdown::{Markdown, MarkdownElement, MarkdownFont, MarkdownStyle};
use nbformat::v4::{CellId, CellMetadata, CellType};
use settings::Settings as _;
use ui::{CommonAnimationExt, IconButtonShape, prelude::*};
use util::ResultExt;
use zed_actions::notebook::InterruptKernel;

use crate::{
    notebook::{CODE_BLOCK_INSET, GUTTER_WIDTH},
    outputs::{Output, plain, plain::TerminalOutput, user_error::ErrorView},
    repl_settings::ReplSettings,
};

#[derive(Copy, Clone, PartialEq, PartialOrd)]
pub enum CellPosition {
    First,
    Middle,
    Last,
}

pub enum CellControlType {
    RunCell,
    RerunCell,
    StopCell,
    ClearCell,
    CellOptions,
    CollapseCell,
    ExpandCell,
}

pub enum CellEvent {
    Run(CellId),
    FocusedIn(CellId),
}

pub enum MarkdownCellEvent {
    FinishedEditing,
    Run(CellId),
}

impl CellControlType {
    fn icon_name(&self) -> IconName {
        match self {
            CellControlType::RunCell => IconName::PlayFilled,
            CellControlType::RerunCell => IconName::ArrowCircle,
            CellControlType::StopCell => IconName::Stop,
            CellControlType::ClearCell => IconName::ListX,
            CellControlType::CellOptions => IconName::Ellipsis,
            CellControlType::CollapseCell => IconName::ChevronDown,
            CellControlType::ExpandCell => IconName::ChevronRight,
        }
    }
    fn id(&self) -> &'static str {
        match self {
            CellControlType::RunCell => "CellControlType::RunCell",
            CellControlType::RerunCell => "CellControlType::RerunCell",
            CellControlType::StopCell => "CellControlType::StopCell",
            CellControlType::ClearCell => "CellControlType::ClearCell",
            CellControlType::CellOptions => "CellControlType::CellOptions",
            CellControlType::CollapseCell => "CellControlType::CollapseCelln",
            CellControlType::ExpandCell => "CellControlType::ExpandCell",
        }
    }
}

pub struct CellControl {
    button: IconButton,
}

impl CellControl {
    fn new(id: impl Into<SharedString>, control_type: CellControlType) -> Self {
        let icon_name = control_type.icon_name();
        let id = id.into();
        let button = IconButton::new(id, icon_name)
            .icon_size(IconSize::Small)
            .shape(IconButtonShape::Square);
        Self { button }
    }
}

impl Clickable for CellControl {
    fn on_click(
        self,
        handler: impl Fn(&gpui::ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        let button = self.button.on_click(handler);
        Self { button }
    }

    fn cursor_style(self, _cursor_style: gpui::CursorStyle) -> Self {
        self
    }
}

/// A notebook cell
#[derive(Clone)]
pub enum Cell {
    Code(Entity<CodeCell>),
    Markdown(Entity<MarkdownCell>),
    Raw(Entity<RawCell>),
}

pub(crate) enum MovementDirection {
    Start,
    End,
}

// The renderer chooses one MIME representation and may normalize terminal text.
// Keep the notebook data separately so saving never depends on that choice.
struct NotebookOutput {
    data: nbformat::v4::Output,
    display_id: Option<String>,
    view: Output,
}

impl NotebookOutput {
    fn new(
        data: nbformat::v4::Output,
        display_id: Option<String>,
        window: &mut Window,
        cx: &mut App,
    ) -> Self {
        let view = match &data {
            nbformat::v4::Output::Stream { text, .. } => Output::Stream {
                content: cx.new(|cx| TerminalOutput::from(&text.0, window, cx)),
            },
            nbformat::v4::Output::DisplayData(display_data) => {
                Output::new(&display_data.data, None, window, cx)
            }
            nbformat::v4::Output::ExecuteResult(execute_result) => {
                Output::new(&execute_result.data, None, window, cx)
            }
            nbformat::v4::Output::Error(error) => Output::ErrorOutput(ErrorView {
                ename: error.ename.clone(),
                evalue: error.evalue.clone(),
                traceback: cx
                    .new(|cx| TerminalOutput::from(&error.traceback.join("\n"), window, cx)),
            }),
        };
        Self {
            data,
            display_id,
            view,
        }
    }
}

impl Cell {
    pub fn id(&self, cx: &App) -> CellId {
        match self {
            Cell::Code(code_cell) => code_cell.read(cx).id().clone(),
            Cell::Markdown(markdown_cell) => markdown_cell.read(cx).id().clone(),
            Cell::Raw(raw_cell) => raw_cell.read(cx).id().clone(),
        }
    }

    pub fn current_source(&self, cx: &App) -> String {
        match self {
            Cell::Code(code_cell) => code_cell.read(cx).current_source(cx),
            Cell::Markdown(markdown_cell) => markdown_cell.read(cx).current_source(cx),
            Cell::Raw(raw_cell) => raw_cell.read(cx).source.clone(),
        }
    }

    pub fn to_nbformat_cell(&self, cx: &App) -> nbformat::v4::Cell {
        match self {
            Cell::Code(code_cell) => code_cell.read(cx).to_nbformat_cell(cx),
            Cell::Markdown(markdown_cell) => markdown_cell.read(cx).to_nbformat_cell(cx),
            Cell::Raw(raw_cell) => raw_cell.read(cx).to_nbformat_cell(),
        }
    }

    pub fn is_dirty(&self, cx: &App) -> bool {
        match self {
            Cell::Code(code_cell) => code_cell.read(cx).is_dirty(cx),
            Cell::Markdown(markdown_cell) => markdown_cell.read(cx).is_dirty(cx),
            Cell::Raw(_) => false,
        }
    }

    pub fn load(
        cell: &nbformat::v4::Cell,
        languages: &Arc<LanguageRegistry>,
        notebook_language: Shared<Task<Option<Arc<Language>>>>,
        window: &mut Window,
        cx: &mut App,
    ) -> Self {
        match cell {
            nbformat::v4::Cell::Markdown {
                id,
                metadata,
                source,
                attachments,
            } => {
                let source = source.concat();

                let entity = cx.new(|cx| {
                    let mut cell = MarkdownCell::new(
                        id.clone(),
                        metadata.clone(),
                        source,
                        languages.clone(),
                        window,
                        cx,
                    );
                    cell.attachments = attachments.clone();
                    cell
                });

                Cell::Markdown(entity)
            }
            nbformat::v4::Cell::Code {
                id,
                metadata,
                execution_count,
                source,
                outputs,
            } => {
                let text = source.concat();

                Cell::Code(cx.new(|cx| {
                    CodeCell::new(
                        CellSource::Existing {
                            execution_count: *execution_count,
                            outputs: outputs.clone(),
                        },
                        id.clone(),
                        metadata.clone(),
                        text,
                        notebook_language,
                        window,
                        cx,
                    )
                }))
            }
            nbformat::v4::Cell::Raw {
                id,
                metadata,
                source,
            } => Cell::Raw(cx.new(|_| RawCell {
                id: id.clone(),
                metadata: metadata.clone(),
                source: source.concat(),
                selected: false,
                cell_position: None,
            })),
        }
    }

    pub(crate) fn move_to(&self, direction: MovementDirection, window: &mut Window, cx: &mut App) {
        fn move_in_editor(
            editor: &Entity<Editor>,
            direction: MovementDirection,
            window: &mut Window,
            cx: &mut App,
        ) {
            editor.update(cx, |editor, cx| {
                match direction {
                    MovementDirection::Start => {
                        editor.move_to_beginning(&Default::default(), window, cx);
                    }
                    MovementDirection::End => {
                        editor.move_to_end(&Default::default(), window, cx);
                    }
                }
                editor.focus_handle(cx).focus(window, cx);
            })
        }

        match self {
            Cell::Code(cell) => {
                cell.update(cx, |cell, cx| {
                    move_in_editor(&cell.editor, direction, window, cx)
                });
            }
            Cell::Markdown(cell) => {
                cell.update(cx, |cell, cx| {
                    cell.set_editing(true);
                    move_in_editor(&cell.editor, direction, window, cx);

                    cx.notify();
                });
            }
            _ => {}
        }
    }

    pub(crate) fn editor<'a>(&'a self, cx: &'a App) -> Option<&'a Entity<Editor>> {
        match self {
            Cell::Code(cell) => Some(cell.read(cx).editor()),
            Cell::Markdown(cell) => Some(cell.read(cx).editor()),
            _ => None,
        }
    }
}

pub trait RenderableCell: Render {
    const CELL_TYPE: CellType;

    fn id(&self) -> &CellId;
    fn cell_type(&self) -> CellType;
    fn metadata(&self) -> &CellMetadata;
    fn source(&self) -> &String;
    fn selected(&self) -> bool;
    fn set_selected(&mut self, selected: bool) -> &mut Self;
    fn selected_bg_color(&self, _window: &mut Window, cx: &mut Context<Self>) -> Hsla {
        if self.selected() {
            let mut color = cx.theme().colors().element_hover;
            color.fade_out(0.5);
            color
        } else {
            // Not sure if this is correct, previous was TODO: this is wrong
            gpui::transparent_black()
        }
    }
    fn control(&self, _window: &mut Window, _cx: &mut Context<Self>) -> Option<CellControl> {
        None
    }

    fn cell_position_spacer(
        &self,
        is_first: bool,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<impl IntoElement> {
        let cell_position = self.cell_position();

        if (cell_position == Some(&CellPosition::First) && is_first)
            || (cell_position == Some(&CellPosition::Last) && !is_first)
        {
            Some(div().flex().w_full().h(DynamicSpacing::Base12.px(cx)))
        } else {
            None
        }
    }

    fn gutter(&self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_selected = self.selected();

        div()
            .relative()
            .h_full()
            .w(px(GUTTER_WIDTH))
            .child(
                div()
                    .w(px(GUTTER_WIDTH))
                    .flex()
                    .flex_none()
                    .justify_center()
                    .h_full()
                    .child(
                        div()
                            .flex_none()
                            .w(px(1.))
                            .h_full()
                            .when(is_selected, |this| this.bg(cx.theme().colors().icon_accent))
                            .when(!is_selected, |this| this.bg(cx.theme().colors().border)),
                    ),
            )
            .when_some(self.control(window, cx), |this, control| {
                this.child(
                    div()
                        .absolute()
                        .top(px(CODE_BLOCK_INSET - 2.0))
                        .left_0()
                        .flex()
                        .flex_none()
                        .w(px(GUTTER_WIDTH))
                        .h(px(GUTTER_WIDTH + 12.0))
                        .items_center()
                        .justify_center()
                        .bg(cx.theme().colors().tab_bar_background)
                        .child(control.button),
                )
            })
    }

    fn cell_position(&self) -> Option<&CellPosition>;
    fn set_cell_position(&mut self, position: CellPosition) -> &mut Self;
}

pub trait RunnableCell: RenderableCell {
    fn execution_count(&self) -> Option<i32>;
    fn set_execution_count(&mut self, count: i32) -> &mut Self;
    fn run(&mut self, window: &mut Window, cx: &mut Context<Self>) -> ();
}

pub struct MarkdownCell {
    id: CellId,
    metadata: CellMetadata,
    attachments: Option<serde_json::Value>,
    image_cache: Entity<RetainAllImageCache>,
    source: String,
    editor: Entity<Editor>,
    markdown: Entity<Markdown>,
    editing: bool,
    selected: bool,
    cell_position: Option<CellPosition>,
    _editor_subscription: gpui::Subscription,
}

impl EventEmitter<MarkdownCellEvent> for MarkdownCell {}

impl MarkdownCell {
    pub fn new(
        id: CellId,
        metadata: CellMetadata,
        source: String,
        languages: Arc<LanguageRegistry>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let buffer = cx.new(|cx| Buffer::local(source.clone(), cx));
        let multi_buffer = cx.new(|cx| MultiBuffer::singleton(buffer.clone(), cx));

        let markdown_language = languages.language_for_name("Markdown");
        cx.spawn_in(window, async move |_this, cx| {
            if let Some(markdown) = markdown_language.await.log_err() {
                buffer.update(cx, |buffer, cx| {
                    buffer.set_language(Some(markdown), cx);
                });
            }
        })
        .detach();

        let editor = cx.new(|cx| {
            let mut editor = Editor::new(
                EditorMode::Full {
                    scale_ui_elements_with_buffer_font_size: false,
                    show_active_line_background: false,
                    sizing_behavior: SizingBehavior::SizeByContent,
                },
                multi_buffer,
                None,
                window,
                cx,
            );

            editor.set_show_gutter(false, cx);
            editor.set_use_modal_editing(true);
            editor.disable_mouse_wheel_zoom();
            editor.disable_scrollbars_and_minimap(window, cx);
            editor
        });

        let markdown = cx.new(|cx| Markdown::new(source.clone().into(), None, None, cx));

        let editor_subscription =
            cx.subscribe(&editor, move |this, _editor, event, cx| match event {
                editor::EditorEvent::Blurred => {
                    if this.editing {
                        this.editing = false;
                        cx.emit(MarkdownCellEvent::FinishedEditing);
                        cx.notify();
                    }
                }
                _ => {}
            });

        let start_editing = source.is_empty();
        Self {
            id,
            metadata,
            attachments: None,
            image_cache: RetainAllImageCache::new(cx),
            source,
            editor,
            markdown,
            editing: start_editing,
            selected: false,
            cell_position: None,
            _editor_subscription: editor_subscription,
        }
    }

    pub fn editor(&self) -> &Entity<Editor> {
        &self.editor
    }

    pub fn current_source(&self, cx: &App) -> String {
        let editor = self.editor.read(cx);
        let buffer = editor.buffer().read(cx);
        buffer
            .as_singleton()
            .map(|b| b.read(cx).text())
            .unwrap_or_default()
    }

    pub fn is_dirty(&self, cx: &App) -> bool {
        self.editor.read(cx).buffer().read(cx).is_dirty(cx)
    }

    pub fn to_nbformat_cell(&self, cx: &App) -> nbformat::v4::Cell {
        let source = self.current_source(cx);
        let source_lines = source.split_inclusive('\n').map(str::to_owned).collect();

        nbformat::v4::Cell::Markdown {
            id: self.id.clone(),
            metadata: self.metadata.clone(),
            source: source_lines,
            attachments: self.attachments.clone(),
        }
    }

    pub fn is_editing(&self) -> bool {
        self.editing
    }

    pub fn set_editing(&mut self, editing: bool) {
        self.editing = editing;
    }

    pub fn reparse_markdown(&mut self, cx: &mut Context<Self>) {
        let editor = self.editor.read(cx);
        let buffer = editor.buffer().read(cx);
        let source = buffer
            .as_singleton()
            .map(|b| b.read(cx).text())
            .unwrap_or_default();

        self.source = source.clone();
        self.markdown.update(cx, |markdown, cx| {
            markdown.reset(source.into(), cx);
        });
    }

    /// Called when user presses Shift+Enter or Ctrl+Enter while editing.
    /// Finishes editing and signals to move to the next cell.
    pub fn run(&mut self, cx: &mut Context<Self>) {
        if self.editing {
            self.editing = false;
            cx.emit(MarkdownCellEvent::FinishedEditing);
            cx.emit(MarkdownCellEvent::Run(self.id.clone()));
            cx.notify();
        }
    }
}

impl RenderableCell for MarkdownCell {
    const CELL_TYPE: CellType = CellType::Markdown;

    fn id(&self) -> &CellId {
        &self.id
    }

    fn cell_type(&self) -> CellType {
        CellType::Markdown
    }

    fn metadata(&self) -> &CellMetadata {
        &self.metadata
    }

    fn source(&self) -> &String {
        &self.source
    }

    fn selected(&self) -> bool {
        self.selected
    }

    fn set_selected(&mut self, selected: bool) -> &mut Self {
        self.selected = selected;
        self
    }

    fn control(&self, _window: &mut Window, _: &mut Context<Self>) -> Option<CellControl> {
        None
    }

    fn cell_position(&self) -> Option<&CellPosition> {
        self.cell_position.as_ref()
    }

    fn set_cell_position(&mut self, cell_position: CellPosition) -> &mut Self {
        self.cell_position = Some(cell_position);
        self
    }
}

impl Render for MarkdownCell {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // If editing, show the editor
        if self.editing {
            return v_flex()
                .size_full()
                .children(self.cell_position_spacer(true, window, cx))
                .child(
                    h_flex()
                        .w_full()
                        .pr_6()
                        .rounded_xs()
                        .items_start()
                        .gap(DynamicSpacing::Base08.rems(cx))
                        .bg(self.selected_bg_color(window, cx))
                        .child(self.gutter(window, cx))
                        .child(
                            div()
                                .flex_1()
                                .p_3()
                                .bg(cx.theme().colors().editor_background)
                                .rounded_sm()
                                .child(self.editor.clone())
                                .on_mouse_down(
                                    gpui::MouseButton::Left,
                                    cx.listener(|_this, _event, _window, _cx| {
                                        // Prevent the click from propagating
                                    }),
                                ),
                        ),
                )
                .children(self.cell_position_spacer(false, window, cx));
        }

        // Preview mode - show rendered markdown

        let style = MarkdownStyle::themed(MarkdownFont::Preview, window, cx);

        v_flex()
            .size_full()
            .children(self.cell_position_spacer(true, window, cx))
            .child(
                h_flex()
                    .w_full()
                    .pr_6()
                    .rounded_xs()
                    .items_start()
                    .gap(DynamicSpacing::Base08.rems(cx))
                    .bg(self.selected_bg_color(window, cx))
                    .child(self.gutter(window, cx))
                    .child(
                        v_flex()
                            .image_cache(self.image_cache.clone())
                            .id("markdown-content")
                            .size_full()
                            .flex_1()
                            .p_3()
                            .font_ui(cx)
                            .text_size(TextSize::Default.rems(cx))
                            .cursor_pointer()
                            .on_click(cx.listener(|this, _event, window, cx| {
                                this.editing = true;
                                window.focus(&this.editor.focus_handle(cx), cx);
                                cx.notify();
                            }))
                            .child(MarkdownElement::new(self.markdown.clone(), style)),
                    ),
            )
            .children(self.cell_position_spacer(false, window, cx))
    }
}

pub struct CodeCell {
    id: CellId,
    metadata: CellMetadata,
    execution_count: Option<i32>,
    source: String,
    editor: Entity<editor::Editor>,
    outputs: Vec<NotebookOutput>,
    output_revision: u64,
    saved_output_revision: u64,
    clear_before_next_output: bool,
    selected: bool,
    cell_position: Option<CellPosition>,
    _language_task: Task<()>,
    execution_start_time: Option<Instant>,
    execution_duration: Option<Duration>,
    is_executing: bool,
}

impl EventEmitter<CellEvent> for CodeCell {}

pub(super) enum CellSource {
    /// Crate a new empty cell
    None,
    /// Backed by an existing notebook cell
    Existing {
        execution_count: Option<i32>,
        outputs: Vec<nbformat::v4::Output>,
    },
}

impl CellSource {
    fn into_outputs(self) -> (Option<i32>, Vec<nbformat::v4::Output>) {
        match self {
            CellSource::Existing {
                execution_count,
                outputs,
            } => (execution_count, outputs),
            CellSource::None => Default::default(),
        }
    }
}

impl CodeCell {
    pub(super) fn new(
        cell_source: CellSource,
        id: CellId,
        metadata: CellMetadata,
        source: String,
        notebook_language: Shared<Task<Option<Arc<Language>>>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let buffer = cx.new(|cx| Buffer::local(source.clone(), cx));
        let multi_buffer = cx.new(|cx| MultiBuffer::singleton(buffer.clone(), cx));

        let editor = cx.new(|cx| {
            let mut editor = Editor::new(
                EditorMode::Full {
                    scale_ui_elements_with_buffer_font_size: false,
                    show_active_line_background: false,
                    sizing_behavior: SizingBehavior::SizeByContent,
                },
                multi_buffer,
                None,
                window,
                cx,
            );

            editor.disable_mouse_wheel_zoom();
            editor.disable_scrollbars_and_minimap(window, cx);
            editor.set_show_gutter(false, cx);
            editor.set_use_modal_editing(true);
            editor
        });

        let language_task = cx.spawn_in(window, async move |_this, cx| {
            let language = notebook_language.await;
            buffer.update(cx, |buffer, cx| {
                buffer.set_language(language.clone(), cx);
            });
        });

        let (execution_count, outputs) = cell_source.into_outputs();
        let outputs = outputs
            .into_iter()
            .map(|output| NotebookOutput::new(output, None, window, cx))
            .collect();

        Self {
            id,
            metadata,
            execution_count,
            source,
            editor,
            outputs,
            output_revision: 0,
            saved_output_revision: 0,
            clear_before_next_output: false,
            selected: false,
            cell_position: None,
            execution_start_time: None,
            execution_duration: None,
            is_executing: false,
            _language_task: language_task,
        }
    }

    pub fn set_language(&mut self, language: Option<Arc<Language>>, cx: &mut Context<Self>) {
        self.editor.update(cx, |editor, cx| {
            editor.buffer().update(cx, |buffer, cx| {
                if let Some(buffer) = buffer.as_singleton() {
                    buffer.update(cx, |buffer, cx| {
                        buffer.set_language(language, cx);
                    });
                }
            });
        });
    }

    pub fn editor(&self) -> &Entity<editor::Editor> {
        &self.editor
    }

    pub fn current_source(&self, cx: &App) -> String {
        let editor = self.editor.read(cx);
        let buffer = editor.buffer().read(cx);
        buffer
            .as_singleton()
            .map(|b| b.read(cx).text())
            .unwrap_or_default()
    }

    pub fn is_dirty(&self, cx: &App) -> bool {
        self.output_revision != self.saved_output_revision
            || self.editor.read(cx).buffer().read(cx).is_dirty(cx)
    }

    pub(super) fn output_revision(&self) -> u64 {
        self.output_revision
    }

    pub(super) fn mark_outputs_saved(&mut self, revision: u64) {
        self.saved_output_revision = revision;
    }

    pub fn to_nbformat_cell(&self, cx: &App) -> nbformat::v4::Cell {
        let source = self.current_source(cx);
        let source_lines = source.split_inclusive('\n').map(str::to_owned).collect();
        let outputs = self
            .outputs
            .iter()
            .map(|output| output.data.clone())
            .collect();

        nbformat::v4::Cell::Code {
            id: self.id.clone(),
            metadata: self.metadata.clone(),
            execution_count: self.execution_count,
            source: source_lines,
            outputs,
        }
    }

    pub fn has_outputs(&self) -> bool {
        !self.outputs.is_empty()
    }

    pub fn clear_outputs(&mut self) {
        if !self.outputs.is_empty() {
            self.output_revision += 1;
        }
        self.outputs.clear();
        self.clear_before_next_output = false;
        self.execution_duration = None;
    }

    fn push_output(
        &mut self,
        data: nbformat::v4::Output,
        display_id: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.clear_before_next_output {
            self.clear_outputs();
        }
        self.outputs
            .push(NotebookOutput::new(data, display_id, window, cx));
        self.output_revision += 1;
    }

    pub fn start_execution(&mut self) {
        self.execution_start_time = Some(Instant::now());
        self.execution_duration = None;
        self.is_executing = true;
    }

    pub fn finish_execution(&mut self) {
        if let Some(start_time) = self.execution_start_time.take() {
            self.execution_duration = Some(start_time.elapsed());
        }
        self.is_executing = false;
    }

    pub fn is_executing(&self) -> bool {
        self.is_executing
    }

    /// Displays a kernel-level failure (e.g. the kernel failed to launch because
    /// Python is not installed) as an error output on this cell, so the user gets
    /// feedback instead of a spinner that never resolves.
    pub fn show_kernel_error(
        &mut self,
        error_message: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.push_output(
            nbformat::v4::Output::Error(nbformat::v4::ErrorOutput {
                ename: "Kernel Error".to_string(),
                evalue: "cell could not be executed".to_string(),
                traceback: vec![error_message.to_string()],
            }),
            None,
            window,
            cx,
        );
        self.execution_start_time = None;
        self.is_executing = false;
        cx.notify();
    }

    pub fn execution_duration(&self) -> Option<Duration> {
        self.execution_duration
    }

    fn format_duration(duration: Duration) -> String {
        let total_secs = duration.as_secs_f64();
        if total_secs < 1.0 {
            format!("{:.0}ms", duration.as_millis())
        } else if total_secs < 60.0 {
            format!("{:.1}s", total_secs)
        } else {
            let minutes = (total_secs / 60.0).floor() as u64;
            let secs = total_secs % 60.0;
            format!("{}m {:.1}s", minutes, secs)
        }
    }

    pub fn handle_message(
        &mut self,
        message: &JupyterMessage,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match &message.content {
            JupyterMessageContent::StreamContent(stream) => {
                self.push_output(
                    nbformat::v4::Output::Stream {
                        name: match stream.name {
                            jupyter_protocol::Stdio::Stdout => "stdout",
                            jupyter_protocol::Stdio::Stderr => "stderr",
                        }
                        .into(),
                        text: nbformat::v4::MultilineString(stream.text.clone()),
                    },
                    None,
                    window,
                    cx,
                );
            }
            JupyterMessageContent::DisplayData(display_data) => {
                self.push_output(
                    nbformat::v4::Output::DisplayData(nbformat::v4::DisplayData {
                        data: display_data.data.clone(),
                        metadata: display_data.metadata.clone(),
                    }),
                    display_data
                        .transient
                        .as_ref()
                        .and_then(|t| t.display_id.clone()),
                    window,
                    cx,
                );
            }
            JupyterMessageContent::ExecuteResult(execute_result) => {
                self.push_output(
                    nbformat::v4::Output::ExecuteResult(nbformat::v4::ExecuteResult {
                        execution_count: execute_result.execution_count,
                        data: execute_result.data.clone(),
                        metadata: execute_result.metadata.clone(),
                    }),
                    execute_result
                        .transient
                        .as_ref()
                        .and_then(|t| t.display_id.clone()),
                    window,
                    cx,
                );
            }
            JupyterMessageContent::ExecuteInput(input) => {
                self.execution_count = i32::try_from(input.execution_count.value()).ok();
                self.output_revision += 1;
            }
            JupyterMessageContent::ExecuteReply(_) => {
                self.finish_execution();
            }
            JupyterMessageContent::ErrorOutput(error) => {
                self.push_output(
                    nbformat::v4::Output::Error(nbformat::v4::ErrorOutput {
                        ename: error.ename.clone(),
                        evalue: error.evalue.clone(),
                        traceback: error.traceback.clone(),
                    }),
                    None,
                    window,
                    cx,
                );
            }
            JupyterMessageContent::ClearOutput(clear) => {
                if clear.wait {
                    self.clear_before_next_output = true;
                } else {
                    self.clear_outputs();
                }
            }
            JupyterMessageContent::UpdateDisplayData(update) => {
                if let Some(display_id) = &update.transient.display_id {
                    for output in &mut self.outputs {
                        if output.display_id.as_ref() != Some(display_id) {
                            continue;
                        }
                        let (data, metadata) = match &mut output.data {
                            nbformat::v4::Output::DisplayData(output) => {
                                (&mut output.data, &mut output.metadata)
                            }
                            nbformat::v4::Output::ExecuteResult(output) => {
                                (&mut output.data, &mut output.metadata)
                            }
                            _ => continue,
                        };
                        *data = update.data.clone();
                        *metadata = update.metadata.clone();
                        output.view = Output::new(data, None, window, cx);
                        self.output_revision += 1;
                    }
                }
            }
            _ => {}
        }
        cx.notify();
    }

    pub fn gutter_output(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_selected = self.selected();

        div()
            .relative()
            .h_full()
            .w(px(GUTTER_WIDTH))
            .child(
                div()
                    .w(px(GUTTER_WIDTH))
                    .flex()
                    .flex_none()
                    .justify_center()
                    .h_full()
                    .child(
                        div()
                            .flex_none()
                            .w(px(1.))
                            .h_full()
                            .when(is_selected, |this| this.bg(cx.theme().colors().icon_accent))
                            .when(!is_selected, |this| this.bg(cx.theme().colors().border)),
                    ),
            )
            .when(self.has_outputs(), |this| {
                this.child(
                    div()
                        .absolute()
                        .top(px(CODE_BLOCK_INSET - 2.0))
                        .left_0()
                        .flex()
                        .flex_none()
                        .w(px(GUTTER_WIDTH))
                        .h(px(GUTTER_WIDTH + 12.0))
                        .items_center()
                        .justify_center()
                        .bg(cx.theme().colors().tab_bar_background)
                        .child(IconButton::new("control", IconName::Ellipsis)),
                )
            })
    }
}

impl RenderableCell for CodeCell {
    const CELL_TYPE: CellType = CellType::Code;

    fn id(&self) -> &CellId {
        &self.id
    }

    fn cell_type(&self) -> CellType {
        CellType::Code
    }

    fn metadata(&self) -> &CellMetadata {
        &self.metadata
    }

    fn source(&self) -> &String {
        &self.source
    }

    fn control(&self, _window: &mut Window, cx: &mut Context<Self>) -> Option<CellControl> {
        let control_type = if self.is_executing {
            CellControlType::StopCell
        } else if self.has_outputs() {
            CellControlType::RerunCell
        } else {
            CellControlType::RunCell
        };

        Some(
            CellControl::new(control_type.id(), control_type).on_click(cx.listener(
                move |this, _, window, cx| {
                    if this.is_executing {
                        window.dispatch_action(Box::new(InterruptKernel), cx);
                    } else {
                        this.run(window, cx);
                    }
                },
            )),
        )
    }

    fn selected(&self) -> bool {
        self.selected
    }

    fn set_selected(&mut self, selected: bool) -> &mut Self {
        self.selected = selected;
        self
    }

    fn cell_position(&self) -> Option<&CellPosition> {
        self.cell_position.as_ref()
    }

    fn set_cell_position(&mut self, cell_position: CellPosition) -> &mut Self {
        self.cell_position = Some(cell_position);
        self
    }

    fn gutter(&self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_selected = self.selected();
        let execution_count = self.execution_count;

        div()
            .relative()
            .h_full()
            .w(px(GUTTER_WIDTH))
            .child(
                div()
                    .w(px(GUTTER_WIDTH))
                    .flex()
                    .flex_none()
                    .justify_center()
                    .h_full()
                    .child(
                        div()
                            .flex_none()
                            .w(px(1.))
                            .h_full()
                            .when(is_selected, |this| this.bg(cx.theme().colors().icon_accent))
                            .when(!is_selected, |this| this.bg(cx.theme().colors().border)),
                    ),
            )
            .when_some(self.control(window, cx), |this, control| {
                this.child(
                    div()
                        .absolute()
                        .top(px(CODE_BLOCK_INSET - 2.0))
                        .left_0()
                        .flex()
                        .flex_col()
                        .w(px(GUTTER_WIDTH))
                        .items_center()
                        .justify_center()
                        .bg(cx.theme().colors().tab_bar_background)
                        .child(control.button)
                        .when_some(execution_count, |this, count| {
                            this.child(
                                div()
                                    .mt_1()
                                    .text_xs()
                                    .text_color(cx.theme().colors().text_muted)
                                    .child(format!("{}", count)),
                            )
                        }),
                )
            })
    }
}

impl RunnableCell for CodeCell {
    fn run(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        cx.emit(CellEvent::Run(self.id.clone()));
    }

    fn execution_count(&self) -> Option<i32> {
        self.execution_count.filter(|&count| count > 0)
    }

    fn set_execution_count(&mut self, count: i32) -> &mut Self {
        self.execution_count = Some(count);
        self
    }
}

impl Render for CodeCell {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let output_max_height = ReplSettings::get_global(cx).output_max_height_lines;
        let output_max_height = if output_max_height > 0 {
            Some(window.line_height() * output_max_height as f32)
        } else {
            None
        };
        let output_max_width =
            plain::max_width_for_columns(ReplSettings::get_global(cx).max_columns, window, cx);
        // get the language from the editor's buffer
        let language_name = self
            .editor
            .read(cx)
            .buffer()
            .read(cx)
            .as_singleton()
            .and_then(|buffer| buffer.read(cx).language())
            .map(|lang| lang.name().to_string());

        v_flex()
            .size_full()
            // TODO: Move base cell render into trait impl so we don't have to repeat this
            .children(self.cell_position_spacer(true, window, cx))
            // Editor portion
            .child(
                h_flex()
                    .w_full()
                    .pr_6()
                    .rounded_xs()
                    .items_start()
                    .gap(DynamicSpacing::Base08.rems(cx))
                    .bg(self.selected_bg_color(window, cx))
                    .child(self.gutter(window, cx))
                    .child(
                        div().py_1p5().w_full().child(
                            div()
                                .relative()
                                .flex()
                                .size_full()
                                .flex_1()
                                .py_3()
                                .px_5()
                                .rounded_lg()
                                .border_1()
                                .border_color(cx.theme().colors().border)
                                .bg(cx.theme().colors().editor_background)
                                .child(div().w_full().child(self.editor.clone()))
                                // lang badge in top-right corner
                                .when_some(language_name, |this, name| {
                                    this.child(
                                        div()
                                            .absolute()
                                            .top_1()
                                            .right_2()
                                            .px_2()
                                            .py_0p5()
                                            .rounded_md()
                                            .bg(cx.theme().colors().element_background.opacity(0.7))
                                            .text_xs()
                                            .text_color(cx.theme().colors().text_muted)
                                            .child(name),
                                    )
                                }),
                        ),
                    ),
            )
            .when(
                self.has_outputs() || self.execution_duration.is_some() || self.is_executing,
                |this| {
                    let execution_time_label = self.execution_duration.map(Self::format_duration);
                    let is_executing = self.is_executing;
                    this.child(
                        h_flex()
                            .w_full()
                            .pr_6()
                            .rounded_xs()
                            .items_start()
                            .gap(DynamicSpacing::Base08.rems(cx))
                            .bg(self.selected_bg_color(window, cx))
                            .child(self.gutter_output(window, cx))
                            .child(
                                div().py_1p5().w_full().child(
                                    v_flex()
                                        .size_full()
                                        .flex_1()
                                        .py_3()
                                        .px_5()
                                        .rounded_lg()
                                        .border_1()
                                        // execution status/time at the TOP
                                        .when(
                                            is_executing || execution_time_label.is_some(),
                                            |this| {
                                                let time_element = if is_executing {
                                                    h_flex()
                                                        .gap_1()
                                                        .items_center()
                                                        .child(
                                                            Icon::new(IconName::ArrowCircle)
                                                                .size(IconSize::XSmall)
                                                                .color(Color::Warning)
                                                                .with_rotate_animation(2)
                                                                .into_any_element(),
                                                        )
                                                        .child(
                                                            div()
                                                                .text_xs()
                                                                .text_color(
                                                                    cx.theme().colors().text_muted,
                                                                )
                                                                .child("Running..."),
                                                        )
                                                        .into_any_element()
                                                } else if let Some(duration_text) =
                                                    execution_time_label.clone()
                                                {
                                                    h_flex()
                                                        .gap_1()
                                                        .items_center()
                                                        .child(
                                                            Icon::new(IconName::Check)
                                                                .size(IconSize::XSmall)
                                                                .color(Color::Success),
                                                        )
                                                        .child(
                                                            div()
                                                                .text_xs()
                                                                .text_color(
                                                                    cx.theme().colors().text_muted,
                                                                )
                                                                .child(duration_text),
                                                        )
                                                        .into_any_element()
                                                } else {
                                                    div().into_any_element()
                                                };
                                                this.child(div().mb_2().child(time_element))
                                            },
                                        )
                                        // output at bottom
                                        .child(
                                            div()
                                                .id((
                                                    ElementId::from(self.id.to_string()),
                                                    "output-scroll",
                                                ))
                                                .w_full()
                                                .when_some(output_max_width, |div, max_width| {
                                                    div.max_w(max_width).overflow_x_scroll()
                                                })
                                                .when_some(output_max_height, |div, max_height| {
                                                    div.max_h(max_height).overflow_y_scroll()
                                                })
                                                .children(self.outputs.iter().map(|output| {
                                                    div().children(output.view.content(window, cx))
                                                })),
                                        ),
                                ),
                            ),
                    )
                },
            )
            // TODO: Move base cell render into trait impl so we don't have to repeat this
            .children(self.cell_position_spacer(false, window, cx))
    }
}

pub struct RawCell {
    id: CellId,
    metadata: CellMetadata,
    source: String,
    selected: bool,
    cell_position: Option<CellPosition>,
}

impl RawCell {
    pub fn to_nbformat_cell(&self) -> nbformat::v4::Cell {
        let source_lines = self
            .source
            .split_inclusive('\n')
            .map(str::to_owned)
            .collect();

        nbformat::v4::Cell::Raw {
            id: self.id.clone(),
            metadata: self.metadata.clone(),
            source: source_lines,
        }
    }
}

impl RenderableCell for RawCell {
    const CELL_TYPE: CellType = CellType::Raw;

    fn id(&self) -> &CellId {
        &self.id
    }

    fn cell_type(&self) -> CellType {
        CellType::Raw
    }

    fn metadata(&self) -> &CellMetadata {
        &self.metadata
    }

    fn source(&self) -> &String {
        &self.source
    }

    fn selected(&self) -> bool {
        self.selected
    }

    fn set_selected(&mut self, selected: bool) -> &mut Self {
        self.selected = selected;
        self
    }

    fn cell_position(&self) -> Option<&CellPosition> {
        self.cell_position.as_ref()
    }

    fn set_cell_position(&mut self, cell_position: CellPosition) -> &mut Self {
        self.cell_position = Some(cell_position);
        self
    }
}

impl Render for RawCell {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .size_full()
            // TODO: Move base cell render into trait impl so we don't have to repeat this
            .children(self.cell_position_spacer(true, window, cx))
            .child(
                h_flex()
                    .w_full()
                    .pr_2()
                    .rounded_xs()
                    .items_start()
                    .gap(DynamicSpacing::Base08.rems(cx))
                    .bg(self.selected_bg_color(window, cx))
                    .child(self.gutter(window, cx))
                    .child(
                        div()
                            .flex()
                            .size_full()
                            .flex_1()
                            .p_3()
                            .font_ui(cx)
                            .text_size(TextSize::Default.rems(cx))
                            .child(self.source.clone()),
                    ),
            )
            // TODO: Move base cell render into trait impl so we don't have to repeat this
            .children(self.cell_position_spacer(false, window, cx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use gpui::{TestAppContext, VisualTestContext};
    use serde_json::json;
    use settings::SettingsStore;

    fn init(cx: &mut TestAppContext) -> &mut VisualTestContext {
        cx.update(|cx| {
            let settings = SettingsStore::test(cx);
            cx.set_global(settings);
            theme_settings::init(theme::LoadThemes::JustBase, cx);
            editor::init(cx);
        });
        cx.add_empty_window()
    }

    fn load(value: serde_json::Value, cx: &mut VisualTestContext) -> Cell {
        let cell = serde_json::from_value(value).unwrap();
        cx.update(|window, cx| {
            Cell::load(
                &cell,
                &Arc::new(LanguageRegistry::new(cx.background_executor().clone())),
                Task::ready(None).shared(),
                window,
                cx,
            )
        })
    }

    fn rich_data() -> serde_json::Value {
        json!({
            "text/plain": "fallback\nwithout trailing newline",
            "text/markdown": "**rich**",
            "text/html": "<b>rich</b>",
            "application/json": {"nested": [1, "λ", null]},
            "image/png": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aR2QAAAAASUVORK5CYII=",
            "image/svg+xml": "<svg xmlns=\"http://www.w3.org/2000/svg\"/>",
            "application/x-notebook-test+json": {"unrendered": true}
        })
    }

    #[gpui::test]
    fn saved_cells_preserve_source_attachments_and_all_output_data(cx: &mut TestAppContext) {
        let cx = init(cx);
        let outputs = json!([
            {"output_type":"stream", "name":"stderr", "text":["\u{1b}[31merror\r", "λ\u{1b}[0m"]},
            {"output_type":"display_data", "data":rich_data(), "metadata":{"image/png":{"width":23}, "custom":{"a":1}}},
            {"output_type":"execute_result", "execution_count":42, "data":rich_data(), "metadata":{"isolated":true}},
            {"output_type":"error", "ename":"ValueError", "evalue":"λ", "traceback":["\u{1b}[31mtrace\nline", "last"]}
        ]);
        for value in [
            json!({"cell_type":"code", "id":"code", "metadata":{"tags":["keep"]}, "source":["x = 1\n", "x"], "execution_count":42, "outputs":outputs}),
            json!({"cell_type":"markdown", "id":"markdown", "metadata":{}, "source":["![plot](attachment:plot.png)"], "attachments":{"plot.png":rich_data()}}),
            json!({"cell_type":"raw", "id":"raw", "metadata":{}, "source":["raw\n", "tail"]}),
            json!({"cell_type":"raw", "id":"empty", "metadata":{}, "source":[]}),
        ] {
            let expected: nbformat::v4::Cell = serde_json::from_value(value.clone()).unwrap();
            let cell = load(value, cx);
            cx.update(|_, cx| {
                assert_eq!(
                    serde_json::to_value(cell.to_nbformat_cell(cx)).unwrap(),
                    serde_json::to_value(expected).unwrap()
                );
                assert!(!cell.is_dirty(cx));
            });
        }
    }

    #[gpui::test]
    fn kernel_outputs_preserve_data_and_apply_display_updates_and_clears(cx: &mut TestAppContext) {
        let cx = init(cx);
        let Cell::Code(cell) = load(
            json!({"cell_type":"code", "id":"code", "metadata":{}, "source":[], "execution_count":null, "outputs":[]}),
            cx,
        ) else {
            panic!()
        };
        cell.update_in(cx, |cell, window, cx| {
            let display: jupyter_protocol::DisplayData = serde_json::from_value(json!({"data":rich_data(), "metadata":{"keep":1}, "transient":{"display_id":"shared"}})).unwrap();
            cell.handle_message(&display.into(), window, cx);
            let result: jupyter_protocol::ExecuteResult = serde_json::from_value(json!({"data":rich_data(), "metadata":{"keep":2}, "execution_count":17, "transient":{"display_id":"shared"}})).unwrap();
            cell.handle_message(&result.into(), window, cx);
            let expected = json!([
                {"output_type":"display_data", "data":rich_data(), "metadata":{"keep":1}},
                {"output_type":"execute_result", "data":rich_data(), "metadata":{"keep":2}, "execution_count":17}
            ]);
            let expected: Vec<nbformat::v4::Output> = serde_json::from_value(expected).unwrap();
            assert_eq!(serde_json::to_value(cell.to_nbformat_cell(cx)).unwrap()["outputs"], serde_json::to_value(expected).unwrap());
            let saved_revision = cell.output_revision();
            cell.mark_outputs_saved(saved_revision);
            assert!(!cell.is_dirty(cx));

            let update: jupyter_protocol::UpdateDisplayData = serde_json::from_value(json!({"data":{"text/plain":"updated", "application/x-notebook-test+json":{"new":true}}, "metadata":{"changed":true}, "transient":{"display_id":"shared"}})).unwrap();
            cell.handle_message(&update.into(), window, cx);
            let saved = serde_json::to_value(cell.to_nbformat_cell(cx)).unwrap();
            assert_eq!(saved["outputs"][0]["metadata"], json!({"changed":true}));
            assert_eq!(saved["outputs"][0]["data"], saved["outputs"][1]["data"]);
            assert_eq!(saved["outputs"][1]["execution_count"], 17);
            assert_eq!(saved["outputs"][0]["data"]["application/x-notebook-test+json"], json!({"new":true}));
            // Completing an older save must not mark this new output clean.
            cell.mark_outputs_saved(saved_revision);
            assert!(cell.is_dirty(cx));

            cell.handle_message(&jupyter_protocol::ClearOutput { wait: true }.into(), window, cx);
            assert_eq!(cell.outputs.len(), 2);
            cell.handle_message(&jupyter_protocol::StreamContent::stderr("final\rλ").into(), window, cx);
            let saved = serde_json::to_value(cell.to_nbformat_cell(cx)).unwrap();
            assert_eq!(saved["outputs"], json!([{"output_type":"stream", "name":"stderr", "text":["final\rλ"]}]));
            cell.handle_message(&jupyter_protocol::ClearOutput { wait: false }.into(), window, cx);
            assert!(!cell.has_outputs());
            cell.handle_message(&jupyter_protocol::ExecuteInput { code:String::new(), execution_count:23.into() }.into(), window, cx);
            assert_eq!(cell.execution_count, Some(23));
        });
    }
}
