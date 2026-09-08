use std::option::Option;

/// An LSP completion.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Completion {
    pub label: String,
    pub label_details: Option<CompletionLabelDetails>,
    pub detail: Option<String>,
    pub kind: Option<CompletionKind>,
    pub insert_text_format: Option<InsertTextFormat>,
}

/// The kind of an LSP completion.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum CompletionKind {
    Text,
    Method,
    Function,
    Constructor,
    Field,
    Variable,
    Class,
    Interface,
    Module,
    Property,
    Unit,
    Value,
    Enum,
    Keyword,
    Snippet,
    Color,
    File,
    Reference,
    Folder,
    EnumMember,
    Constant,
    Struct,
    Event,
    Operator,
    TypeParameter,
    Other(i32),
}

/// Label details for an LSP completion.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompletionLabelDetails {
    pub detail: Option<String>,
    pub description: Option<String>,
}

/// Defines how to interpret the insert text in a completion item.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum InsertTextFormat {
    PlainText,
    Snippet,
    Other(i32),
}

/// An LSP symbol.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Symbol {
    pub kind: SymbolKind,
    pub name: String,
    pub container_name: Option<String>,
}

/// The kind of an LSP symbol.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum SymbolKind {
    File,
    Module,
    Namespace,
    Package,
    Class,
    Method,
    Property,
    Field,
    Constructor,
    Enum,
    Interface,
    Function,
    Variable,
    Constant,
    String,
    Number,
    Boolean,
    Array,
    Object,
    Key,
    Null,
    EnumMember,
    Struct,
    Event,
    Operator,
    TypeParameter,
    Other(i32),
}

/// Browser presentation callbacks execute in the sandbox's existing extension host.
#[derive(serde::Serialize, serde::Deserialize)]
pub enum LanguageServerLabelRequest {
    Completions(Vec<Completion>),
    Symbols(Vec<Symbol>),
}

pub type RemoteLanguageServerLabels = std::sync::Arc<
    dyn Fn(
            ::lsp::LanguageServerName,
            LanguageServerLabelRequest,
        )
            -> futures::future::BoxFuture<'static, anyhow::Result<Vec<Option<super::CodeLabel>>>>
        + Send
        + Sync,
>;
