//! Shared browser presence identity for avatars and cursor labels.

use clock::ReplicaId;

/// The sandbox assigns at most 32 distinct replicas, retained across reconnects.
/// Each has a unique animal; no account, random client state or network lookup.
pub fn identity(replica: ReplicaId) -> (&'static str, &'static str) {
    const ANIMALS: [(&str, &str); 32] = [
        ("Anonymous Owl", "images/animals/owl.svg"),
        ("Anonymous Fox", "images/animals/fox.svg"),
        ("Anonymous Panda", "images/animals/panda.svg"),
        ("Anonymous Octopus", "images/animals/octopus.svg"),
        ("Anonymous Turtle", "images/animals/turtle.svg"),
        ("Anonymous Cat", "images/animals/cat.svg"),
        ("Anonymous Rabbit", "images/animals/rabbit.svg"),
        ("Anonymous Koala", "images/animals/koala.svg"),
        ("Anonymous Penguin", "images/animals/penguin.svg"),
        ("Anonymous Dolphin", "images/animals/dolphin.svg"),
        ("Anonymous Dog", "images/animals/dog.svg"),
        ("Anonymous Lion", "images/animals/lion.svg"),
        ("Anonymous Tiger", "images/animals/tiger.svg"),
        ("Anonymous Bear", "images/animals/bear.svg"),
        ("Anonymous Frog", "images/animals/frog.svg"),
        ("Anonymous Monkey", "images/animals/monkey.svg"),
        ("Anonymous Duck", "images/animals/duck.svg"),
        ("Anonymous Butterfly", "images/animals/butterfly.svg"),
        ("Anonymous Bee", "images/animals/bee.svg"),
        ("Anonymous Snail", "images/animals/snail.svg"),
        ("Anonymous Ladybug", "images/animals/ladybug.svg"),
        ("Anonymous Mouse", "images/animals/mouse.svg"),
        ("Anonymous Hamster", "images/animals/hamster.svg"),
        ("Anonymous Wolf", "images/animals/wolf.svg"),
        ("Anonymous Horse", "images/animals/horse.svg"),
        ("Anonymous Unicorn", "images/animals/unicorn.svg"),
        ("Anonymous Pig", "images/animals/pig.svg"),
        ("Anonymous Cow", "images/animals/cow.svg"),
        ("Anonymous Chicken", "images/animals/chicken.svg"),
        ("Anonymous Bat", "images/animals/bat.svg"),
        ("Anonymous Bird", "images/animals/bird.svg"),
        ("Anonymous Boar", "images/animals/boar.svg"),
    ];
    ANIMALS[replica
        .as_u16()
        .saturating_sub(ReplicaId::FIRST_COLLAB_ID.as_u16()) as usize
        % ANIMALS.len()]
}
