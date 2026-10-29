use super::{TableCursor, MAX_INPUTS};
use std::cmp::Ordering;

#[derive(Clone, Copy, Default)]
struct Head {
    winner: usize,
    // All sources whose head has the minimum key, including older versions.
    equal: u16,
}

// Only source indices and a duplicate mask are retained. Keys and values stay
// borrowed from the cursors until the output writer has consumed the winner.
pub(super) struct Tournament {
    nodes: [Head; MAX_INPUTS * 2],
    leaves: usize,
    stale: bool,
}

impl Tournament {
    pub fn new(cursors: &[TableCursor]) -> Self {
        assert!(cursors.len() <= MAX_INPUTS);
        let mut out = Self {
            nodes: [Head::default(); MAX_INPUTS * 2],
            leaves: cursors.len().next_power_of_two(),
            stale: false,
        };
        out.rebuild::<true>(cursors);
        out
    }

    pub fn is_stale(&self) -> bool {
        self.stale
    }

    pub fn invalidate(&mut self) {
        self.stale = true;
    }

    pub fn rebuild<const PREFIX: bool>(&mut self, cursors: &[TableCursor]) {
        for i in 0..cursors.len() {
            self.update_leaf(i, cursors);
        }
        for node in (1..self.leaves).rev() {
            self.combine::<PREFIX>(node, cursors);
        }
        self.stale = false;
    }

    pub fn update_leaf(&mut self, source: usize, cursors: &[TableCursor]) -> u32 {
        self.nodes[self.leaves + source] = Head {
            winner: source,
            equal: if cursors[source].current().is_some() {
                1 << source
            } else {
                0
            },
        };
        let mut node = (self.leaves + source) / 2;
        let mut dirty = 0;
        while node != 0 {
            dirty |= 1 << node;
            node /= 2;
        }
        dirty
    }

    pub fn repair<const PREFIX: bool>(&mut self, mut dirty: u32, cursors: &[TableCursor]) {
        // Repair each ancestor once even when every source advances together.
        // This bounds duplicate-heavy updates to at most N-1 key comparisons.
        while dirty != 0 {
            let node = (u32::BITS - 1 - dirty.leading_zeros()) as usize;
            dirty &= !(1 << node);
            self.combine::<PREFIX>(node, cursors);
        }
    }

    fn combine<const PREFIX: bool>(&mut self, node: usize, cursors: &[TableCursor]) {
        let left = self.nodes[node * 2];
        let right = self.nodes[node * 2 + 1];
        self.nodes[node] = if left.equal == 0 {
            right
        } else if right.equal == 0 {
            left
        } else {
            let a = cursors[left.winner].current_for::<PREFIX>().unwrap();
            let b = cursors[right.winner].current_for::<PREFIX>().unwrap();
            match a.key.cmp(b.key) {
                Ordering::Less => left,
                Ordering::Greater => right,
                Ordering::Equal => Head {
                    winner: if a.seq > b.seq || (a.seq == b.seq && left.winner < right.winner) {
                        left.winner
                    } else {
                        right.winner
                    },
                    equal: left.equal | right.equal,
                },
            }
        };
    }

    pub fn selected(&self, pending: &mut Vec<usize>) -> Option<usize> {
        let head = self.nodes[1];
        let mut equal = head.equal;
        while equal != 0 {
            let source = equal.trailing_zeros() as usize;
            equal &= equal - 1;
            pending.push(source);
        }
        (head.equal != 0).then_some(head.winner)
    }
}
