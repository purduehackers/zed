//! Expose GPUI's AccessKit tree through browser-native accessibility semantics.

use crate::events::EventListenerHandle;
use accesskit::{Action, ActionRequest, Node, NodeId, Role, TreeId, TreeUpdate};
use gpui::A11yCallbacks;
use std::{
    cell::Cell,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
};
use wasm_bindgen::JsCast;

thread_local! {
    static SCREEN_READER_MODE: Cell<bool> = Cell::new(web_sys::window()
        .and_then(|window| window.local_storage().ok().flatten())
        .and_then(|storage| storage.get_item("gpui-screen-reader-mode").ok().flatten())
        .as_deref() == Some("true"));
}

pub(crate) fn screen_reader_mode() -> bool {
    SCREEN_READER_MODE.get()
}

/// Supplies the focused text control's name without changing the editor model.
pub fn set_text_input_label(label: &str, read_only: bool) {
    if let Some(element) = web_sys::window()
        .and_then(|window| window.document())
        .and_then(|document| document.query_selector("[data-gpui-input]").ok().flatten())
    {
        element.set_attribute("aria-label", label).ok();
        element
            .set_attribute("aria-readonly", bool_str(read_only))
            .ok();
    }
}

/// Enables full-document text access and Tab navigation out of the editor.
pub fn toggle_screen_reader_mode() -> bool {
    let enabled = !screen_reader_mode();
    SCREEN_READER_MODE.set(enabled);
    if let Some(window) = web_sys::window() {
        if let Ok(Some(storage)) = window.local_storage() {
            storage
                .set_item("gpui-screen-reader-mode", bool_str(enabled))
                .ok();
        }
        if let Ok(event) = web_sys::Event::new("gpui-screen-reader-mode") {
            window.dispatch_event(&event).ok();
        }
    }
    enabled
}

pub(crate) struct WebAccessibility {
    root: web_sys::HtmlElement,
    focus_ring: web_sys::HtmlElement,
    nodes: BTreeMap<NodeId, (Node, web_sys::HtmlElement)>,
    root_id: NodeId,
    focus: NodeId,
    callbacks: Rc<A11yCallbacks>,
    _listeners: Vec<EventListenerHandle>,
}

impl WebAccessibility {
    pub(crate) fn new(
        document: &web_sys::Document,
        callbacks: A11yCallbacks,
    ) -> Result<Self, wasm_bindgen::JsValue> {
        let root: web_sys::HtmlElement = document.create_element("div")?.dyn_into()?;
        root.set_attribute("data-gpui-a11y", "")?;
        // Transparent, not hidden: screen readers must see the real hierarchy.
        // Pointer interaction continues to use GPUI's canvas hit testing.
        root.style()
            .set_css_text("position:fixed;inset:0;opacity:0;pointer-events:none;overflow:hidden");
        document.body().unwrap().append_child(&root)?;
        let focus_ring: web_sys::HtmlElement = document.create_element("div")?.dyn_into()?;
        focus_ring.set_attribute("aria-hidden", "true")?;
        focus_ring.style().set_css_text("position:fixed;display:none;pointer-events:none;outline:2px solid Highlight;outline-offset:-2px;z-index:2147483647");
        document.body().unwrap().append_child(&focus_ring)?;
        let callbacks = Rc::new(callbacks);
        let mut listeners = Vec::new();
        for (event_name, action) in [("click", Action::Click), ("focusin", Action::Focus)] {
            let callbacks = callbacks.clone();
            let focus_ring = focus_ring.clone();
            listeners.push(EventListenerHandle::add(
                root.as_ref(),
                event_name,
                move |event| {
                    let event: web_sys::Event = event.unchecked_into();
                    let target = event
                        .target()
                        .and_then(|target| target.dyn_into::<web_sys::Element>().ok());
                    let node =
                        target.and_then(|target| target.closest("[data-gpui-node]").ok().flatten());
                    if let Some(node) = &node {
                        if node.get_attribute("aria-disabled").as_deref() == Some("true") {
                            return;
                        }
                        if action == Action::Focus {
                            let rect = node.get_bounding_client_rect();
                            for (property, value) in [
                                ("left", rect.left()),
                                ("top", rect.top()),
                                ("width", rect.width()),
                                ("height", rect.height()),
                            ] {
                                focus_ring
                                    .style()
                                    .set_property(property, &format!("{value}px"))
                                    .ok();
                            }
                            focus_ring.style().set_property("display", "block").ok();
                        }
                    }
                    if let Some(id) = node
                        .and_then(|node| node.get_attribute("data-gpui-node"))
                        .and_then(|id| id.parse().ok())
                    {
                        (callbacks.action)(ActionRequest {
                            action,
                            target_tree: TreeId::ROOT,
                            target_node: NodeId(id),
                            data: None,
                        });
                    }
                },
            ));
        }
        let ring = focus_ring.clone();
        listeners.push(EventListenerHandle::add(
            root.as_ref(),
            "focusout",
            move |_| {
                ring.style().set_property("display", "none").ok();
            },
        ));
        let action_callbacks = callbacks.clone();
        listeners.push(EventListenerHandle::add(
            root.as_ref(),
            "keydown",
            move |event| {
                let event: web_sys::KeyboardEvent = event.unchecked_into();
                if matches!(
                    event.key().as_str(),
                    "ArrowLeft" | "ArrowRight" | "Home" | "End"
                ) && !event.alt_key()
                    && !event.ctrl_key()
                    && !event.meta_key()
                {
                    let target = event
                        .target()
                        .and_then(|target| target.dyn_into::<web_sys::Element>().ok())
                        .filter(|target| target.get_attribute("role").as_deref() == Some("tab"));
                    if let Some(target) = target
                        && let Ok(Some(list)) = target.closest("[role=tablist]")
                    {
                        let descendants = list.get_elements_by_tag_name("div");
                        let tabs = (0..descendants.length())
                            .filter_map(|ix| descendants.item(ix))
                            .filter(|element| {
                                element.get_attribute("role").as_deref() == Some("tab")
                            })
                            .collect::<Vec<_>>();
                        if let Some(current) = tabs.iter().position(|tab| *tab == target) {
                            let next = match event.key().as_str() {
                                "Home" => 0,
                                "End" => tabs.len() - 1,
                                "ArrowLeft" => (current + tabs.len() - 1) % tabs.len(),
                                _ => (current + 1) % tabs.len(),
                            };
                            if let Some(id) = tabs[next]
                                .get_attribute("data-gpui-node")
                                .and_then(|id| id.parse().ok())
                            {
                                (action_callbacks.action)(ActionRequest {
                                    action: Action::Click,
                                    target_tree: TreeId::ROOT,
                                    target_node: NodeId(id),
                                    data: None,
                                });
                            }
                            if let Some(tab) = tabs[next].dyn_ref::<web_sys::HtmlElement>() {
                                tab.focus().ok();
                            }
                            event.prevent_default();
                            event.stop_propagation();
                            return;
                        }
                    }
                }
                if event.key() == "Tab"
                    && !event.alt_key()
                    && !event.ctrl_key()
                    && !event.meta_key()
                {
                    let target = event
                        .target()
                        .and_then(|target| target.dyn_into::<web_sys::Element>().ok());
                    if let Some(dialog) = target.and_then(|target| {
                        target
                            .closest("[role=dialog], [role=alertdialog]")
                            .ok()
                            .flatten()
                    }) {
                        let descendants = dialog.get_elements_by_tag_name("div");
                        let buttons = (0..descendants.length())
                            .filter_map(|ix| descendants.item(ix))
                            .filter_map(|element| element.dyn_into::<web_sys::HtmlElement>().ok())
                            .filter(|element| {
                                element.tab_index() == 0 && element.has_attribute("data-gpui-click")
                            })
                            .collect::<Vec<_>>();
                        let active = dialog
                            .owner_document()
                            .and_then(|document| document.active_element());
                        let current = buttons
                            .iter()
                            .position(|button| active.as_ref() == Some(button.as_ref()));
                        let next = if event.shift_key() {
                            current
                                .unwrap_or(0)
                                .checked_sub(1)
                                .unwrap_or(buttons.len().saturating_sub(1))
                        } else {
                            current.map_or(0, |ix| (ix + 1) % buttons.len())
                        };
                        if let Some(button) = buttons.get(next) {
                            button.focus().ok();
                        }
                        event.prevent_default();
                        event.stop_propagation();
                        return;
                    }
                }
                if !matches!(event.key().as_str(), "Enter" | " ")
                    || event.alt_key()
                    || event.ctrl_key()
                    || event.meta_key()
                {
                    return;
                }
                let target = event
                    .target()
                    .and_then(|target| target.dyn_into::<web_sys::Element>().ok());
                if let Some(target) =
                    target.filter(|target| target.has_attribute("data-gpui-click"))
                    && let Some(id) = target
                        .get_attribute("data-gpui-node")
                        .and_then(|id| id.parse().ok())
                {
                    event.prevent_default();
                    event.stop_propagation();
                    (action_callbacks.action)(ActionRequest {
                        action: Action::Click,
                        target_tree: TreeId::ROOT,
                        target_node: NodeId(id),
                        data: None,
                    });
                }
            },
        ));
        let mut this = Self {
            root,
            focus_ring,
            nodes: BTreeMap::new(),
            root_id: NodeId(0),
            focus: NodeId(0),
            callbacks,
            _listeners: listeners,
        };
        if let Some(tree) = (this.callbacks.activation)() {
            this.update(tree, 1.0);
        }
        Ok(this)
    }

    pub(crate) fn update(&mut self, update: TreeUpdate, scale: f32) {
        if let Some(tree) = update.tree {
            self.root_id = tree.root;
        }
        let document = self.root.owner_document().unwrap();
        let active = document.active_element();
        let had_semantic_focus = active
            .as_ref()
            .is_some_and(|active| self.root.contains(Some(active.as_ref())));
        for (id, node) in update.nodes {
            let element = if let Some((previous, element)) = self.nodes.get(&id) {
                if *previous == node {
                    continue;
                }
                element.clone()
            } else {
                let Ok(element) = document
                    .create_element("div")
                    .and_then(|element| Ok(element.dyn_into::<web_sys::HtmlElement>()?))
                else {
                    continue;
                };
                element.set_id(&format!("gpui-a11y-{}", id.0));
                element
                    .set_attribute("data-gpui-node", &id.0.to_string())
                    .ok();
                element
                    .style()
                    .set_css_text("position:fixed;overflow:hidden;white-space:pre-wrap");
                element
            };
            attr(&element, "role", role(node.role()));
            attr(
                &element,
                "aria-modal",
                matches!(node.role(), Role::Dialog | Role::AlertDialog).then_some("true"),
            );
            attr(&element, "aria-label", node.label());
            attr(&element, "aria-description", node.description());
            attr(&element, "aria-keyshortcuts", node.keyboard_shortcut());
            attr(
                &element,
                "data-gpui-click",
                node.supports_action(Action::Click).then_some(""),
            );
            attr(
                &element,
                "aria-disabled",
                node.is_disabled().then_some("true"),
            );
            attr(&element, "aria-hidden", node.is_hidden().then_some("true"));
            attr(
                &element,
                "aria-readonly",
                node.is_read_only().then_some("true"),
            );
            attr(&element, "aria-selected", node.is_selected().map(bool_str));
            attr(&element, "aria-expanded", node.is_expanded().map(bool_str));
            let toggled = node.toggled().map(|value| match value {
                accesskit::Toggled::True => "true",
                accesskit::Toggled::False => "false",
                accesskit::Toggled::Mixed => "mixed",
            });
            let button = role(node.role()) == Some("button");
            attr(&element, "aria-pressed", toggled.filter(|_| button));
            attr(&element, "aria-checked", toggled.filter(|_| !button));
            attr(
                &element,
                "aria-level",
                node.level().map(|level| level.to_string()).as_deref(),
            );
            attr(
                &element,
                "aria-posinset",
                node.position_in_set().map(|n| n.to_string()).as_deref(),
            );
            attr(
                &element,
                "aria-setsize",
                node.size_of_set().map(|n| n.to_string()).as_deref(),
            );
            attr(
                &element,
                "aria-valuenow",
                node.numeric_value()
                    .map(|value| value.to_string())
                    .as_deref(),
            );
            attr(
                &element,
                "aria-valuemin",
                node.min_numeric_value()
                    .map(|value| value.to_string())
                    .as_deref(),
            );
            attr(
                &element,
                "aria-valuemax",
                node.max_numeric_value()
                    .map(|value| value.to_string())
                    .as_deref(),
            );
            attr(
                &element,
                "aria-multiline",
                (node.role() == Role::MultilineTextInput).then_some("true"),
            );
            for (name, ids) in [
                ("aria-labelledby", node.labelled_by()),
                ("aria-describedby", node.described_by()),
                ("aria-controls", node.controls()),
            ] {
                let ids = ids
                    .iter()
                    .map(|id| format!("gpui-a11y-{}", id.0))
                    .collect::<Vec<_>>()
                    .join(" ");
                attr(&element, name, (!ids.is_empty()).then_some(ids.as_str()));
            }
            let focusable =
                node.supports_action(Action::Focus) || node.supports_action(Action::Click);
            element.set_tab_index(if focusable && !node.is_hidden() && !node.is_disabled() {
                0
            } else {
                -1
            });
            if let Some(bounds) = node.bounds() {
                let scale = f64::from(scale.max(0.1));
                for (property, value) in [
                    ("left", bounds.x0 / scale),
                    ("top", bounds.y0 / scale),
                    ("width", bounds.width() / scale),
                    ("height", bounds.height() / scale),
                ] {
                    element
                        .style()
                        .set_property(property, &format!("{value}px"))
                        .ok();
                }
            }
            if node.children().is_empty() {
                element.set_text_content(node.value());
            }
            self.nodes.insert(id, (node, element));
        }
        let mut reachable = BTreeSet::new();
        let mut pending = vec![(self.root_id, self.root.clone())];
        while let Some((id, parent)) = pending.pop() {
            if !reachable.insert(id) {
                continue;
            }
            let Some((node, element)) = self.nodes.get(&id) else {
                continue;
            };
            if element.parent_element().as_ref() != Some(parent.as_ref()) {
                parent.append_child(element).ok();
            }
            // Only move a child when its order changed, retaining DOM identity,
            // focus and the screen reader's virtual cursor across redraws.
            for (index, child) in node.children().iter().enumerate() {
                if let Some((_, child_element)) = self.nodes.get(child) {
                    let existing = element.children().item(index as u32);
                    if existing.as_ref() != Some(child_element.as_ref()) {
                        element
                            .insert_before(
                                child_element,
                                existing.as_ref().map(|child| child.as_ref()),
                            )
                            .ok();
                    }
                    pending.push((*child, element.clone()));
                }
            }
        }
        self.nodes.retain(|id, (_, element)| {
            if reachable.contains(id) {
                true
            } else {
                element.remove();
                false
            }
        });
        // One tab stop per composite widget. Its query/arrows navigate the rows;
        // opening a large project must not add thousands of Tab presses.
        for (id, (node, element)) in &self.nodes {
            match node.role() {
                Role::ListBox | Role::ListBoxOption => element.set_tab_index(-1),
                Role::TreeItem => element.set_tab_index(if *id == update.focus { 0 } else { -1 }),
                Role::Tree => {
                    let focused_child =
                        self.nodes.get(&update.focus).is_some_and(|(node, child)| {
                            node.role() == Role::TreeItem && element.contains(Some(child.as_ref()))
                        });
                    element.set_tab_index(if focused_child { -1 } else { 0 });
                }
                Role::Tab => element.set_tab_index(if node.is_selected() == Some(true) {
                    0
                } else {
                    -1
                }),
                _ => {}
            }
        }
        // Picker keyboard focus stays in its query editor. Tell the browser
        // which option GPUI selected without moving DOM focus out of that input.
        if let Ok(Some(input)) = document.query_selector("[data-gpui-input]") {
            let controlled = self
                .nodes
                .get(&update.focus)
                .filter(|(node, _)| node.role() == Role::ListBoxOption)
                .and_then(|(_, option)| Some((option, option.closest("[role=listbox]").ok()??)));
            for (name, value) in [
                (
                    "aria-controls",
                    controlled.as_ref().map(|(_, list)| list.id()),
                ),
                (
                    "aria-activedescendant",
                    controlled.as_ref().map(|(option, _)| option.id()),
                ),
            ] {
                if let Some(value) = value {
                    if input.get_attribute(name).as_deref() != Some(value.as_str()) {
                        input.set_attribute(name, &value).ok();
                    }
                } else {
                    input.remove_attribute(name).ok();
                }
            }
        }
        let focused_node_removed =
            had_semantic_focus && active.as_ref().is_some_and(|active| !active.is_connected());
        if self.focus != update.focus || focused_node_removed {
            self.focus = update.focus;
            // Follow GPUI focus only while the user is navigating this semantic
            // tree. Never steal focus from the browser chrome or host forms.
            if active.is_some_and(|active| {
                had_semantic_focus
                    || (active.has_attribute("data-gpui-input")
                        && self.nodes.get(&update.focus).is_some_and(|(node, _)| {
                            matches!(node.role(), Role::Dialog | Role::AlertDialog)
                        }))
            }) {
                let target = self
                    .nodes
                    .get(&update.focus)
                    .filter(|_| update.focus != self.root_id)
                    .map(|(_, element)| element.clone())
                    .or_else(|| {
                        document
                            .query_selector("[data-gpui-input]")
                            .ok()
                            .flatten()
                            .and_then(|element| element.dyn_into().ok())
                    });
                if let Some(target) = target {
                    wasm_bindgen_futures::spawn_local(async move {
                        target.focus().ok();
                    });
                }
            }
        }
    }
}

impl Drop for WebAccessibility {
    fn drop(&mut self) {
        self.root.remove();
        self.focus_ring.remove();
        (self.callbacks.deactivation)();
    }
}

fn attr(element: &web_sys::HtmlElement, name: &str, value: Option<&str>) {
    if element.get_attribute(name).as_deref() == value {
        return;
    }
    if let Some(value) = value {
        element.set_attribute(name, value).ok();
    } else {
        element.remove_attribute(name).ok();
    }
}
fn bool_str(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

fn role(role: Role) -> Option<&'static str> {
    Some(match role {
        Role::Window | Role::Pane | Role::Group | Role::TitleBar => "group",
        Role::Button | Role::DefaultButton | Role::DisclosureTriangle => "button",
        Role::Image => "img",
        Role::Link => "link",
        Role::CheckBox => "checkbox",
        Role::RadioButton => "radio",
        Role::Switch => "switch",
        Role::RadioGroup => "radiogroup",
        Role::Tab => "tab",
        Role::TabList => "tablist",
        Role::TabPanel => "tabpanel",
        Role::Tree => "tree",
        Role::TreeItem => "treeitem",
        Role::TreeGrid => "treegrid",
        Role::List => "list",
        Role::ListItem => "listitem",
        Role::ListBox => "listbox",
        Role::ListBoxOption | Role::MenuListOption => "option",
        Role::Menu => "menu",
        Role::MenuItem => "menuitem",
        Role::MenuItemCheckBox => "menuitemcheckbox",
        Role::MenuItemRadio => "menuitemradio",
        Role::MenuBar => "menubar",
        Role::TextInput | Role::MultilineTextInput => "textbox",
        Role::SearchInput => "searchbox",
        Role::ComboBox | Role::EditableComboBox => "combobox",
        Role::Toolbar => "toolbar",
        Role::Dialog => "dialog",
        Role::AlertDialog => "alertdialog",
        Role::Alert => "alert",
        Role::Status => "status",
        Role::Tooltip => "tooltip",
        Role::ProgressIndicator => "progressbar",
        Role::Slider => "slider",
        Role::SpinButton => "spinbutton",
        Role::ScrollBar => "scrollbar",
        Role::Table => "table",
        Role::Grid => "grid",
        Role::Row => "row",
        Role::Cell => "cell",
        Role::GridCell => "gridcell",
        Role::ColumnHeader => "columnheader",
        Role::RowHeader => "rowheader",
        Role::RowGroup => "rowgroup",
        Role::Heading => "heading",
        Role::Splitter => "separator",
        Role::Document => "document",
        Role::Main => "main",
        Role::Navigation => "navigation",
        Role::Region => "region",
        Role::Log | Role::Terminal => "log",
        _ => return None,
    })
}
