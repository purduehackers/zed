// Browser-owned handles never cross a WASM worker boundary. Only paths and bytes do.
const destinations = new Map();
const maxFileBytes = 8 * 1024 * 1024; // Leave room in the 16 MiB project RPC envelope.
const maxImportBytes = 64 * 1024 * 1024;
const maxEntries = 10000;
let retainedBytes = 0;
let importing = false;

function component(name) {
    if (!name || name === "." || name === ".." || /[\/\\\0]/.test(name)) {
        throw new Error("Invalid file name in browser selection");
    }
    return name;
}

async function collect(roots) {
    if (importing) throw new Error("Another file import is still being read");
    importing = true;
    const base = `/browser/imports/${crypto.randomUUID()}`;
    const entries = [];
    const names = new Set();
    let size = 0;
    try {
        async function add(path, file) {
            path.split("/").forEach(component);
            if (names.has(path)) throw new Error(`Duplicate selected path: ${path}`);
            names.add(path);
            if (names.size > maxEntries) throw new Error("Select at most 10,000 files and folders");
            if (file) {
                size += file.size;
                if (file.size > maxFileBytes) throw new Error(`${path} exceeds the 8 MiB file limit`);
                if (size > maxImportBytes) throw new Error("Select at most 64 MiB at a time");
                if (retainedBytes + size > 2 * maxImportBytes) {
                    throw new Error("This tab has reached its 128 MiB local-file limit. Save your work before reloading.");
                }
            }
            entries.push([`${base}/${path}`, file ? new Uint8Array(await file.arrayBuffer()) : null]);
        }
        async function walk(entry, parent = "") {
            const path = parent + component(entry.name);
            if (entry.kind === "file") {
                await add(path, await entry.getFile());
            } else if (entry.kind === "directory") {
                await add(path, null);
                for await (const child of entry.values()) await walk(child, `${path}/`);
            } else if (entry.isFile) {
                await add(path, await new Promise((resolve, reject) => entry.file(resolve, reject)));
            } else if (entry.isDirectory) {
                await add(path, null);
                const reader = entry.createReader();
                // Chromium returns directories in batches, not all at once.
                for (;;) {
                    const batch = await new Promise((resolve, reject) => reader.readEntries(resolve, reject));
                    if (!batch.length) break;
                    for (const child of batch) await walk(child, `${path}/`);
                }
            } else {
                const relative = entry.webkitRelativePath || path;
                const parts = relative.split("/");
                parts.forEach(component);
                for (let i = 1; i < parts.length; i++) {
                    const directory = parts.slice(0, i).join("/");
                    if (!names.has(directory)) await add(directory, null);
                }
                await add(relative, entry);
            }
        }
        for (const entry of roots) await walk(await entry);
        retainedBytes += size;
        for (const [path, bytes] of entries) {
            // Opening a local file imports a copy; Save downloads that copy. A local
            // source is never overwritten just because it was selected or dropped.
            if (bytes) destinations.set(path, null);
        }
        return entries;
    } finally {
        importing = false;
    }
}

export function droppedFiles(data) {
    // Capture entries synchronously: DataTransfer is protected after the drop handler.
    const roots = Array.from(data.items).filter(item => item.kind === "file").map(item => {
        return item.webkitGetAsEntry?.() || item.getAsFile();
    }).filter(Boolean);
    return collect(roots.length ? roots : Array.from(data.files));
}

export async function pickFiles(files, directories, multiple) {
    try {
        if (!files && !directories) throw new Error("No file or folder selection was requested");
        if (directories && !files && window.showDirectoryPicker) {
            return await collect([await window.showDirectoryPicker({ mode: "read" })]);
        }
        const selected = await new Promise((resolve, reject) => {
            const input = document.createElement("input");
            input.type = "file";
            input.multiple = multiple;
            input.webkitdirectory = directories && !files;
            input.hidden = true;
            document.body.append(input);
            const finish = result => { input.remove(); resolve(result); };
            input.addEventListener("change", () => finish(Array.from(input.files)), { once: true });
            input.addEventListener("cancel", () => finish([]), { once: true });
            try { input.click(); } catch (error) { input.remove(); reject(error); }
        });
        return selected.length ? await collect(selected) : null;
    } catch (error) {
        if (error.name === "AbortError") return null;
        throw error;
    }
}

export async function pickSave(suggestedName) {
    try {
        let handle = null;
        let name;
        if (window.showSaveFilePicker) {
            handle = await window.showSaveFilePicker({ suggestedName });
            name = handle.name;
        } else {
            name = window.prompt("Download file as", suggestedName);
            if (name === null) return null;
        }
        const path = `/browser/downloads/${crypto.randomUUID()}/${component(name)}`;
        destinations.set(path, handle);
        return path;
    } catch (error) {
        if (error.name === "AbortError") return null;
        throw error;
    }
}

export async function saveFile(path, bytes) {
    if (!destinations.has(path)) throw new Error("Choose a browser save destination for this file first");
    const handle = destinations.get(path);
    if (handle) {
        const stream = await handle.createWritable();
        try {
            await stream.write(bytes);
            await stream.close();
        } catch (error) {
            await stream.abort().catch(() => {});
            throw error;
        }
    } else {
        const url = URL.createObjectURL(new Blob([bytes], { type: "application/octet-stream" }));
        const link = document.createElement("a");
        link.href = url;
        link.download = path.split("/").at(-1);
        document.body.append(link);
        link.click();
        link.remove();
        setTimeout(() => URL.revokeObjectURL(url), 60000);
    }
}
