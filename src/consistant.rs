use std::default::Default;
use chashmap::CHashMap;
use crc::crc32::checksum_ieee;

#[derive(Debug)]
pub struct Consistant {
    pub replicas_num: usize,

    circle: CHashMap<u32, String>,
    sorted_keys: Vec<u32>,
    members: Vec<String>,
}

impl Default for Consistant {
    fn default() -> Consistant {
        Consistant {
            replicas_num: 20,
            circle: CHashMap::new(),
            sorted_keys: Vec::new(),
            members: Vec::new(),
        }
    }
}

impl Consistant {
    pub fn new(replicas_num: usize) -> Self {
        Consistant {
            replicas_num: replicas_num,
            circle: CHashMap::new(),
            sorted_keys: Vec::new(),
            members: Vec::new(),
        }
    }

    pub fn members(&self) -> &[String] {
        &self.members
    }

    pub fn count(&self) -> usize {
        self.members.len()
    }

    pub fn add<S: Into<String>>(&mut self, element: S) {
        let s = element.into();

        for i in 0..self.replicas_num {
            let sum = checksum_ieee(Self::generate_element_name(s.clone(), i).as_bytes());
            self.circle.insert(sum, s.clone());

            self.sorted_keys.push(sum);
            self.sorted_keys.sort();
        }

        self.members.push(s);
    }

    pub fn get<S: Into<String>>(&self, name: S) -> Option<String> {
        if self.members.len() == 0 {
            return None;
        }
        let key = self.get_key(checksum_ieee(name.into().as_bytes()));

        match self.circle.get(&key) {
            Some(v) => Some((*v).clone()),
            None => unreachable!(),
        }
    }

    #[inline]
    fn get_key(&self, sum: u32) -> u32 {
        for key in &self.sorted_keys {
            if sum < *key {
                return *key;
            }
        }

        self.sorted_keys[0]
    }

    #[inline]
    fn generate_element_name(element: String, i: usize) -> String {
        element + &i.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        let consistant = Consistant::default();

        assert_eq!(consistant.replicas_num, 20);
        assert_eq!(consistant.circle.len(), 0);
        assert_eq!(consistant.members.len(), 0);
        assert_eq!(consistant.sorted_keys.len(), 0);
    }

    #[test]
    fn test_new() {
        let consistant = Consistant::new(30);

        assert_eq!(consistant.replicas_num, 30);
        assert_eq!(consistant.circle.len(), 0);
        assert_eq!(consistant.members.len(), 0);
        assert_eq!(consistant.sorted_keys.len(), 0);
    }

    #[test]
    fn test_members() {
        let mut consistant = Consistant::default();
        consistant.add("cacheA");
        consistant.add("cacheB");
        assert_eq!(consistant.members.len(), 2);
    }

    #[test]
    fn test_add() {
        let mut consistant = Consistant::default();
        consistant.add("cacheA");
        consistant.add("cacheB");
        consistant.add("cacheC");

        assert_eq!(consistant.circle.len(), 3 * consistant.replicas_num);
        assert_eq!(consistant.sorted_keys.len(), 3 * consistant.replicas_num);
    }

    #[test]
    fn test_get() {
        let mut consistant = Consistant::default();
        consistant.add("cacheA");
        consistant.add("cacheB");
        consistant.add("cacheC");

        assert_eq!(consistant.get("david").unwrap(),
                   consistant.get("david").unwrap());
        assert_eq!(consistant.get("kally").unwrap(),
                   consistant.get("kally").unwrap());
        assert_eq!(consistant.get("jason").unwrap(),
                   consistant.get("jason").unwrap());
    }
}

