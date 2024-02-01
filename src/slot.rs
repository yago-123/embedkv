use std::cmp::Ordering;

#[derive(Debug, Eq, PartialEq, PartialOrd, Clone)]
pub struct Slot {
    pub space: usize,
    pub cursor: usize,
}

impl Ord for Slot {
    fn cmp(&self, other: &Self) -> Ordering {
        let order = self.space.cmp(&other.space);
        if order == Ordering::Equal {
            // todo(): refine the ordering
        }

        return order
    }
}

impl Slot {
    pub(crate) fn is_neighbour_of(&self, spot: &Slot) -> bool {
        let func_is_neighbour = |nb1: &Slot, nb2: &Slot| -> bool {
            nb1.cursor > 0 && (nb1.cursor == nb2.cursor + nb2.space)
        };

        return func_is_neighbour(self, spot) || func_is_neighbour(spot, self);
    }

    pub(crate) fn merge_with(&self, spot: &Slot) -> Slot {
        Slot {
            cursor: self.cursor.min(spot.cursor),
            space: self.space + spot.space,
        }
    }
}