//! Shared browser presence identity for avatars and cursor labels.

use clock::ReplicaId;

/// The sandbox assigns at most 32 distinct replicas, retained across reconnects.
/// Each has a unique animal; no account, random client state or network lookup.
pub fn name(replica: ReplicaId) -> &'static str {
    const ANIMALS: [&str; 32] = [
        "Anonymous Owl",
        "Anonymous Fox",
        "Anonymous Panda",
        "Anonymous Octopus",
        "Anonymous Turtle",
        "Anonymous Cat",
        "Anonymous Rabbit",
        "Anonymous Koala",
        "Anonymous Penguin",
        "Anonymous Dolphin",
        "Anonymous Dog",
        "Anonymous Lion",
        "Anonymous Tiger",
        "Anonymous Bear",
        "Anonymous Frog",
        "Anonymous Monkey",
        "Anonymous Duck",
        "Anonymous Butterfly",
        "Anonymous Bee",
        "Anonymous Snail",
        "Anonymous Ladybug",
        "Anonymous Mouse",
        "Anonymous Hamster",
        "Anonymous Wolf",
        "Anonymous Horse",
        "Anonymous Unicorn",
        "Anonymous Pig",
        "Anonymous Cow",
        "Anonymous Chicken",
        "Anonymous Bat",
        "Anonymous Bird",
        "Anonymous Boar",
    ];
    ANIMALS[replica
        .as_u16()
        .saturating_sub(ReplicaId::FIRST_COLLAB_ID.as_u16()) as usize
        % ANIMALS.len()]
}
