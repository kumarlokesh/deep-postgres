pub const PAGE_SIZE: usize = 8192;

pub struct HeapPage {
    data: Box<[u8; PAGE_SIZE]>,
}

impl HeapPage {
    pub fn new() -> Self {
        Self {
            data: Box::new([0; PAGE_SIZE]),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.data.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_page_is_zeroed_and_full_size() {
        let page = HeapPage::new();
        assert_eq!(page.len(), PAGE_SIZE);
        assert!(page.as_bytes().iter().all(|&b| b == 0));
    }
}
