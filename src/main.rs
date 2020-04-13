use rand::Rng;
use std::collections::HashSet;
use std::iter::FromIterator;
use rdxsort::*;
use std::sync::{Mutex, MutexGuard, LockResult};
use std::thread;
use std::sync::mpsc::channel;

struct Row {
    key: i32,
    value: i32,
    lock: Mutex<(i32)>
}

impl Row {
    pub fn new(key: i32, value: i32) -> Row {
        Row {
            key,
            value,
            lock: Mutex::new(0)
        }
    }

    pub fn get_value(&mut self) -> i32 {
        return self.value;
    }

    pub fn set_value(&mut self, value: i32) {
        self.value = value;
    }

}

fn generate_random_transactions(main_index: &mut Vec<i32>,
                                num_transactions: i32,
                                num_operations: i32) -> Vec<Vec<i32>>{

    let mut random_transactions : Vec<Vec<i32>> = vec![];
    let mut rng = rand::thread_rng();

    let min = *main_index.iter().min().unwrap();
    let mut max = *main_index.iter().max().unwrap();
    max = max + 1;

    for _i in 0..num_transactions {
        let mut read_hashset = HashSet::new();
        for _j in 0..num_operations {
            read_hashset.insert(rng.gen_range(min, max));
        }
        let mut read_set = Vec::from_iter(read_hashset.iter().cloned());
//        read_set.rdxsort();
        random_transactions.push(read_set);
    }
    return random_transactions;
}

fn worker(table: &mut Vec<Mutex<i32>>, random_transactions: &Vec<Vec<i32>>) {


    for i in 0..random_transactions.len() {
        let random_readset_ref = &random_transactions[i];
        let mut random_readset = random_readset_ref.clone();
        let mut sorted_readset = random_readset_ref.clone();
        let mut rows: Vec<&mut Row> = Vec::new();
        //        sort phase
        sorted_readset.rdxsort();

        let mut write_set: Vec<i32> = Vec::new();

//      growing phase
        for index in sorted_readset {
            let mut row =  &table[index as usize];
            let mut guard = *row.lock().unwrap();
            guard = guard + 1;
            write_set.push(guard);
        }
//      critical section
        for mut row in write_set {
            row = row + 1;
        }
        std::mem::drop(acquired_locks);
//      shrinking phase
    }
    //release all the acquired locks
}

fn test(a: Mutex<i32>) {
    a.lock();
} //

fn main() {

    const NUM_TRANSACTIONS: i32 = 100;
    const NUM_ROWS: i32 = 100;
    const NUM_OPERATIONS: i32 = 20;
    const NUM_THREADS: i32 = 1;

    let mut main_index= Vec::new();
    let mut table: Vec<Row> = Vec::new();
    let mut thread_transactions: Vec<Vec<Vec<i32>>> = Vec::new();
    let mut thread_id = 0;

    for i in 0..NUM_ROWS {
        let row = Row::new(i, 0);
        table.push(row);
        main_index.push(i);
    }

    for i in 0..NUM_THREADS {
        thread_transactions.push(generate_random_transactions(&mut main_index, NUM_TRANSACTIONS, NUM_OPERATIONS))
    }

    for i in 0..table.len() {
        let row = &table[i];
    }



//    let random_transactions = generate_random_transactions(&mut main_index,20,20);
//
//    let mut row = Row::new(0, 0);
//    println!("{}", row.get_key());
//    row.set_value(1);
//    print!("{}", row.get_value());

//    for transaction in random_transactions {
//        print!("<");
//        for i in transaction {
//            print!("{}, ", i);
//        }
//        print!(">\n");
//    }
}
