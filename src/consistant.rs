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

    pub fn add<S: Into<String>>(&mut self, element: S) {
        let s = element.into();

        for i in 0..self.replicas_num {
            let sum = checksum_ieee(Self::generate_element_name(s.clone(), i).as_bytes());
            self.circle.insert(sum, s.clone());
            self.members.push(s.clone());

            self.sorted_keys.push(sum);
            self.sorted_keys.sort();
        }
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
            if sum > *key {
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

