use jupyter_protocol::JupyterKernelspec;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct LocalKernelSpecification {
    pub name: String,
    pub path: PathBuf,
    pub kernelspec: JupyterKernelspec,
}

impl PartialEq for LocalKernelSpecification {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.path == other.path
    }
}

impl Eq for LocalKernelSpecification {}

#[derive(Debug, Clone)]
pub struct RemoteKernelSpecification {
    pub name: String,
    pub url: String,
    pub token: String,
    pub kernelspec: JupyterKernelspec,
}

impl PartialEq for RemoteKernelSpecification {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.url == other.url
    }
}

impl Eq for RemoteKernelSpecification {}
