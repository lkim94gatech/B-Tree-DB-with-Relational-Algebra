#include <iostream>
#include <map>
#include <vector>
#include <fstream>
#include <iostream>
#include <chrono>

#include <list>
#include <unordered_map>
#include <iostream>
#include <map>
#include <string>
#include <memory>
#include <sstream>
#include <limits>
#include <thread>
#include <queue>
#include <optional>
#include <regex>
#include <stdexcept>
#include <cassert>
#include <random>
#include <tuple>
#include <unordered_set>
#include <set>

using namespace std::literals::string_literals;

#define UNUSED(p)  ((void)(p))

#define ASSERT_WITH_MESSAGE(condition, message) \
    do { \
        if (!(condition)) { \
            std::cerr << "Assertion \033[1;31mFAILED\033[0m: " << message << " at " << __FILE__ << ":" << __LINE__ << std::endl; \
            std::abort(); \
        } \
    } while(0)


enum FieldType { INT, FLOAT, STRING };

// Define a basic Field variant class that can hold different types
class Field {
public:
    FieldType type;
    size_t data_length;
    std::unique_ptr<char[]> data;

public:
    Field(int i) : type(INT) { 
        data_length = sizeof(int);
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), &i, data_length);
    }

    Field(float f) : type(FLOAT) { 
        data_length = sizeof(float);
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), &f, data_length);
    }

    Field(const std::string& s) : type(STRING) {
        data_length = s.size() + 1;  // include null-terminator
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), s.c_str(), data_length);
    }

    Field& operator=(const Field& other) {
        if (&other == this) {
            return *this;
        }
        type = other.type;
        data_length = other.data_length;
        std::memcpy(data.get(), other.data.get(), data_length);
        return *this;
    }

   // Copy constructor
    Field(const Field& other) : type(other.type), data_length(other.data_length), data(new char[data_length]) {
        std::memcpy(data.get(), other.data.get(), data_length);
    }

    // Move constructor - If you already have one, ensure it's correctly implemented
    Field(Field&& other) noexcept : type(other.type), data_length(other.data_length), data(std::move(other.data)) {
        // Optionally reset other's state if needed
    }

    FieldType getType() const { return type; }
    int asInt() const { 
        return *reinterpret_cast<int*>(data.get());
    }
    float asFloat() const { 
        return *reinterpret_cast<float*>(data.get());
    }
    std::string asString() const { 
        switch (type) {
            case INT:
                return std::to_string(asInt());
            case FLOAT:
                return std::to_string(asFloat());
            case STRING:
                return std::string(data.get());
            default:
                return "";
        }
    }

    std::string serialize() {
        std::stringstream buffer;
        buffer << type << ' ' << data_length << ' ';
        if (type == STRING) {
            buffer << data.get() << ' ';
        } else if (type == INT) {
            buffer << *reinterpret_cast<int*>(data.get()) << ' ';
        } else if (type == FLOAT) {
            buffer << *reinterpret_cast<float*>(data.get()) << ' ';
        }
        return buffer.str();
    }

    void serialize(std::ofstream& out) {
        std::string serializedData = this->serialize();
        out << serializedData;
    }

    static std::unique_ptr<Field> deserialize(std::istream& in) {
        int type; in >> type;
        size_t length; in >> length;
        if (type == STRING) {
            std::string val; in >> val;
            return std::make_unique<Field>(val);
        } else if (type == INT) {
            int val; in >> val;
            return std::make_unique<Field>(val);
        } else if (type == FLOAT) {
            float val; in >> val;
            return std::make_unique<Field>(val);
        }
        return nullptr;
    }

    // Clone method
    std::unique_ptr<Field> clone() const {
        // Use the copy constructor
        return std::make_unique<Field>(*this);
    }

    void print() const{
        switch(getType()){
            case INT: std::cout << asInt(); break;
            case FLOAT: std::cout << asFloat(); break;
            case STRING: std::cout << asString(); break;
        }
    }
};

bool operator==(const Field& lhs, const Field& rhs) {
    if (lhs.type != rhs.type) return false; // Different types are never equal

    switch (lhs.type) {
        case INT:
            return *reinterpret_cast<const int*>(lhs.data.get()) == *reinterpret_cast<const int*>(rhs.data.get());
        case FLOAT:
            return *reinterpret_cast<const float*>(lhs.data.get()) == *reinterpret_cast<const float*>(rhs.data.get());
        case STRING:
            return std::string(lhs.data.get(), lhs.data_length - 1) == std::string(rhs.data.get(), rhs.data_length - 1);
        default:
            throw std::runtime_error("Unsupported field type for comparison.");
    }
}

class Tuple {
public:
    std::vector<std::unique_ptr<Field>> fields;

    void addField(std::unique_ptr<Field> field) {
        fields.push_back(std::move(field));
    }

    size_t getSize() const {
        size_t size = 0;
        for (const auto& field : fields) {
            size += field->data_length;
        }
        return size;
    }

    std::string serialize() {
        std::stringstream buffer;
        buffer << fields.size() << ' ';
        for (const auto& field : fields) {
            buffer << field->serialize();
        }
        return buffer.str();
    }

    void serialize(std::ofstream& out) {
        std::string serializedData = this->serialize();
        out << serializedData;
    }

    static std::unique_ptr<Tuple> deserialize(std::istream& in) {
        auto tuple = std::make_unique<Tuple>();
        size_t fieldCount; in >> fieldCount;
        for (size_t i = 0; i < fieldCount; ++i) {
            tuple->addField(Field::deserialize(in));
        }
        return tuple;
    }

    // Clone method
    std::unique_ptr<Tuple> clone() const {
        auto clonedTuple = std::make_unique<Tuple>();
        for (const auto& field : fields) {
            clonedTuple->addField(field->clone());
        }
        return clonedTuple;
    }

    void print() const {
        for (const auto& field : fields) {
            field->print();
            std::cout << " ";
        }
        std::cout << "\n";
    }
};

static constexpr size_t PAGE_SIZE = 4096;  // Fixed page size
static constexpr size_t MAX_SLOTS = 512;   // Fixed number of slots
uint16_t INVALID_VALUE = std::numeric_limits<uint16_t>::max(); // Sentinel value

struct Slot {
    bool empty = true;                 // Is the slot empty?    
    uint16_t offset = INVALID_VALUE;    // Offset of the slot within the page
    uint16_t length = INVALID_VALUE;    // Length of the slot
};

// Slotted Page class
class SlottedPage {
public:
    std::unique_ptr<char[]> page_data = std::make_unique<char[]>(PAGE_SIZE);
    size_t metadata_size = sizeof(Slot) * MAX_SLOTS;

    SlottedPage(){
        // Empty page -> initialize slot array inside page
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
            slot_array[slot_itr].empty = true;
            slot_array[slot_itr].offset = INVALID_VALUE;
            slot_array[slot_itr].length = INVALID_VALUE;
        }
    }

    // Add a tuple, returns true if it fits, false otherwise.
    bool addTuple(std::unique_ptr<Tuple> tuple) {

        // Serialize the tuple into a char array
        auto serializedTuple = tuple->serialize();
        size_t tuple_size = serializedTuple.size();

        //std::cout << "Tuple size: " << tuple_size << " bytes\n";
        // assert(tuple_size == 38);

        // Check for first slot with enough space
        size_t slot_itr = 0;
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());        
        for (; slot_itr < MAX_SLOTS; slot_itr++) {
            if (slot_array[slot_itr].empty == true and 
                slot_array[slot_itr].length >= tuple_size) {
                break;
            }
        }
        if (slot_itr == MAX_SLOTS){
            //std::cout << "Page does not contain an empty slot with sufficient space to store the tuple.";
            return false;
        }

        // Identify the offset where the tuple will be placed in the page
        // Update slot meta-data if needed
        slot_array[slot_itr].empty = false;
        size_t offset = INVALID_VALUE;
        if (slot_array[slot_itr].offset == INVALID_VALUE){
            if(slot_itr != 0){
                auto prev_slot_offset = slot_array[slot_itr - 1].offset;
                auto prev_slot_length = slot_array[slot_itr - 1].length;
                offset = prev_slot_offset + prev_slot_length;
            }
            else{
                offset = metadata_size;
            }

            slot_array[slot_itr].offset = offset;
        }
        else{
            offset = slot_array[slot_itr].offset;
        }

        if(offset + tuple_size >= PAGE_SIZE){
            slot_array[slot_itr].empty = true;
            slot_array[slot_itr].offset = INVALID_VALUE;
            return false;
        }

        assert(offset != INVALID_VALUE);
        assert(offset >= metadata_size);
        assert(offset + tuple_size < PAGE_SIZE);

        if (slot_array[slot_itr].length == INVALID_VALUE){
            slot_array[slot_itr].length = tuple_size;
        }

        // Copy serialized data into the page
        std::memcpy(page_data.get() + offset, 
                    serializedTuple.c_str(), 
                    tuple_size);

        return true;
    }

    void deleteTuple(size_t index) {
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        size_t slot_itr = 0;
        for (; slot_itr < MAX_SLOTS; slot_itr++) {
            if(slot_itr == index and
               slot_array[slot_itr].empty == false){
                slot_array[slot_itr].empty = true;
                break;
               }
        }

        //std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void print() const{
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
            if (slot_array[slot_itr].empty == false){
                assert(slot_array[slot_itr].offset != INVALID_VALUE);
                const char* tuple_data = page_data.get() + slot_array[slot_itr].offset;
                std::istringstream iss(tuple_data);
                auto loadedTuple = Tuple::deserialize(iss);
                std::cout << "Slot " << slot_itr << " : [";
                std::cout << (uint16_t)(slot_array[slot_itr].offset) << "] :: ";
                loadedTuple->print();
            }
        }
        std::cout << "\n";
    }
};

const std::string database_filename = "buzzdb.dat";

class StorageManager {
public:    
    std::fstream fileStream;
    size_t num_pages = 0;

public:
    StorageManager(){
        fileStream.open(database_filename, std::ios::in | std::ios::out | std::ios::trunc );
        if (!fileStream) {
            // If file does not exist, create it
            fileStream.clear(); // Reset the state
            fileStream.open(database_filename, std::ios::out | std::ios::trunc);
        }
        fileStream.close(); 
        fileStream.open(database_filename, std::ios::in | std::ios::out); 

        fileStream.seekg(0, std::ios::end);
        num_pages = fileStream.tellg() / PAGE_SIZE;

        // std::cout << "Storage Manager :: Num pages: " << num_pages << "\n";        
        if(num_pages == 0){
            extend();
        }

    }

    ~StorageManager() {
        if (fileStream.is_open()) {
            fileStream.close();
        }
    }

    // Read a page from disk
    std::unique_ptr<SlottedPage> load(uint16_t page_id) {
        fileStream.seekg(page_id * PAGE_SIZE, std::ios::beg);
        auto page = std::make_unique<SlottedPage>();
        // Read the content of the file into the page
        if(fileStream.read(page->page_data.get(), PAGE_SIZE)){
            //std::cout << "Page read successfully from file." << std::endl;
        }
        else{
            std::cerr << "Error: Unable to read data from the file. \n";
            exit(-1);
        }
        return page;
    }

    // Write a page to disk
    void flush(uint16_t page_id, const std::unique_ptr<SlottedPage>& page) {
        size_t page_offset = page_id * PAGE_SIZE;        

        // Move the write pointer
        fileStream.seekp(page_offset, std::ios::beg);
        fileStream.write(page->page_data.get(), PAGE_SIZE);        
        fileStream.flush();
    }

    // Extend database file by one page
    void extend() {
        // std::cout << "Extending database file \n";

        // Create a slotted page
        auto empty_slotted_page = std::make_unique<SlottedPage>();

        // Move the write pointer
        fileStream.seekp(0, std::ios::end);

        // Write the page to the file, extending it
        fileStream.write(empty_slotted_page->page_data.get(), PAGE_SIZE);
        fileStream.flush();

        // Update number of pages
        num_pages += 1;
    }

};

using PageID = uint16_t;

class Policy {
public:
    virtual bool touch(PageID page_id) = 0;
    virtual PageID evict() = 0;
    virtual ~Policy() = default;
};

void printList(std::string list_name, const std::list<PageID>& myList) {
        std::cout << list_name << " :: ";
        for (const PageID& value : myList) {
            std::cout << value << ' ';
        }
        std::cout << '\n';
}

class LruPolicy : public Policy {
private:
    // List to keep track of the order of use
    std::list<PageID> lruList;

    // Map to find a page's iterator in the list efficiently
    std::unordered_map<PageID, std::list<PageID>::iterator> map;

    size_t cacheSize;

public:

    LruPolicy(size_t cacheSize) : cacheSize(cacheSize) {}

    bool touch(PageID page_id) override {
        //printList("LRU", lruList);

        bool found = false;
        // If page already in the list, remove it
        if (map.find(page_id) != map.end()) {
            found = true;
            lruList.erase(map[page_id]);
            map.erase(page_id);            
        }

        // If cache is full, evict
        if(lruList.size() == cacheSize){
            evict();
        }

        if(lruList.size() < cacheSize){
            // Add the page to the front of the list
            lruList.emplace_front(page_id);
            map[page_id] = lruList.begin();
        }

        return found;
    }

    PageID evict() override {
        // Evict the least recently used page
        PageID evictedPageId = INVALID_VALUE;
        if(lruList.size() != 0){
            evictedPageId = lruList.back();
            map.erase(evictedPageId);
            lruList.pop_back();
        }
        return evictedPageId;
    }

};

constexpr size_t MAX_PAGES_IN_MEMORY = 10;

class BufferManager {
private:
    using PageMap = std::unordered_map<PageID, std::unique_ptr<SlottedPage>>;

    StorageManager storage_manager;
    PageMap pageMap;
    std::unique_ptr<Policy> policy;

public:
    BufferManager(): 
    policy(std::make_unique<LruPolicy>(MAX_PAGES_IN_MEMORY)) {}

    std::unique_ptr<SlottedPage>& getPage(int page_id) {
        auto it = pageMap.find(page_id);
        if (it != pageMap.end()) {
            policy->touch(page_id);
            return pageMap.find(page_id)->second;
        }

        if (pageMap.size() >= MAX_PAGES_IN_MEMORY) {
            auto evictedPageId = policy->evict();
            if(evictedPageId != INVALID_VALUE){
                // std::cout << "Evicting page " << evictedPageId << "\n";
                storage_manager.flush(evictedPageId, 
                                      pageMap[evictedPageId]);
            }
        }

        auto page = storage_manager.load(page_id);
        policy->touch(page_id);
        // std::cout << "Loading page: " << page_id << "\n";
        pageMap[page_id] = std::move(page);
        return pageMap[page_id];
    }

    void flushPage(int page_id) {
        //std::cout << "Flush page " << page_id << "\n";
        storage_manager.flush(page_id, pageMap[page_id]);
    }

    void extend(){
        storage_manager.extend();
    }
    
    size_t getNumPages(){
        return storage_manager.num_pages;
    }

};

class HashIndex {
private:
    struct HashEntry {
        int key;
        int value;
        int position; // Final position within the array
        bool exists; // Flag to check if entry exists

        // Default constructor
        HashEntry() : key(0), value(0), position(-1), exists(false) {}

        // Constructor for initializing with key, value, and exists flag
        HashEntry(int k, int v, int pos) : key(k), value(v), position(pos), exists(true) {}    
    };

    static const size_t capacity = 100; // Hard-coded capacity
    HashEntry hashTable[capacity]; // Static-sized array

    size_t hashFunction(int key) const {
        return key % capacity; // Simple modulo hash function
    }

public:
    HashIndex() {
        // Initialize all entries as non-existing by default
        for (size_t i = 0; i < capacity; ++i) {
            hashTable[i] = HashEntry();
        }
    }

    void insertOrUpdate(int key, int value) {
        size_t index = hashFunction(key);
        size_t originalIndex = index;
        bool inserted = false;
        int i = 0; // Attempt counter

        do {
            if (!hashTable[index].exists) {
                hashTable[index] = HashEntry(key, value, true);
                hashTable[index].position = index;
                inserted = true;
                break;
            } else if (hashTable[index].key == key) {
                hashTable[index].value += value;
                hashTable[index].position = index;
                inserted = true;
                break;
            }
            i++;
            index = (originalIndex + i*i) % capacity; // Quadratic probing
        } while (index != originalIndex && !inserted);

        if (!inserted) {
            std::cerr << "HashTable is full or cannot insert key: " << key << std::endl;
        }
    }

   int getValue(int key) const {
        size_t index = hashFunction(key);
        size_t originalIndex = index;

        do {
            if (hashTable[index].exists && hashTable[index].key == key) {
                return hashTable[index].value;
            }
            if (!hashTable[index].exists) {
                break; // Stop if we find a slot that has never been used
            }
            index = (index + 1) % capacity;
        } while (index != originalIndex);

        return -1; // Key not found
    }

    // This method is not efficient for range queries 
    // as this is an unordered index
    // but is included for comparison
    std::vector<int> rangeQuery(int lowerBound, int upperBound) const {
        std::vector<int> values;
        for (size_t i = 0; i < capacity; ++i) {
            if (hashTable[i].exists && hashTable[i].key >= lowerBound && hashTable[i].key <= upperBound) {
                std::cout << "Key: " << hashTable[i].key << 
                ", Value: " << hashTable[i].value << std::endl;
                values.push_back(hashTable[i].value);
            }
        }
        return values;
    }

    void print() const {
        for (size_t i = 0; i < capacity; ++i) {
            if (hashTable[i].exists) {
                std::cout << "Position: " << hashTable[i].position << 
                ", Key: " << hashTable[i].key << 
                ", Value: " << hashTable[i].value << std::endl;
            }
        }
    }
};

class Operator {
    public:
    virtual ~Operator() = default;

    /// Initializes the operator.
    virtual void open() = 0;

    /// Tries to generate the next tuple. Return true when a new tuple is
    /// available.
    virtual bool next() = 0;

    /// Destroys the operator.
    virtual void close() = 0;

    /// This returns the pointers to the Fields of the generated tuple. When
    /// `next()` returns true, the Fields will contain the values for the
    /// next tuple. Each `Field` pointer in the vector stands for one attribute of the tuple.
    virtual std::vector<std::unique_ptr<Field>> getOutput() = 0;
};

class UnaryOperator : public Operator {
    protected:
    Operator* input;

    public:
    explicit UnaryOperator(Operator& input) : input(&input) {}

    ~UnaryOperator() override = default;
};

class BinaryOperator : public Operator {
    protected:
    Operator* input_left;
    Operator* input_right;

    public:
    explicit BinaryOperator(Operator& input_left, Operator& input_right)
        : input_left(&input_left), input_right(&input_right) {}

    // ~BinaryOperator() override = default;
};

struct FieldVectorHasher {
    std::size_t operator()(const std::vector<Field>& fields) const {
        std::size_t hash = 0;
        for (const auto& field : fields) {
            std::hash<std::string> hasher;
            std::size_t fieldHash = 0;

            // Depending on the type, hash the corresponding data
            switch (field.type) {
                case INT: {
                    // Convert integer data to string and hash
                    int value = *reinterpret_cast<const int*>(field.data.get());
                    fieldHash = hasher(std::to_string(value));
                    break;
                }
                case FLOAT: {
                    // Convert float data to string and hash
                    float value = *reinterpret_cast<const float*>(field.data.get());
                    fieldHash = hasher(std::to_string(value));
                    break;
                }
                case STRING: {
                    // Directly hash the string data
                    std::string value(field.data.get(), field.data_length - 1); // Exclude null-terminator
                    fieldHash = hasher(value);
                    break;
                }
                default:
                    throw std::runtime_error("Unsupported field type for hashing.");
            }

            // Combine the hash of the current field with the hash so far
            hash ^= fieldHash + 0x9e3779b9 + (hash << 6) + (hash >> 2);
        }
        return hash;
    }
};

struct FieldHasher {
    std::size_t operator()(const Field& field) const {
        // TODO: Add your implementation here
        std::hash<std::string> hasher;
        std::size_t hash = 0;

        switch (field.type) {
            case INT: {
                int value = *reinterpret_cast<const int*>(field.data.get());
                hash = hasher(std::to_string(value));
                break;
            }
            case FLOAT: {
                float value = *reinterpret_cast<const float*>(field.data.get());
                hash = hasher(std::to_string(value));
                break;
            }
            case STRING: {
                std::string value(field.data.get(), field.data_length - 1);
                hash = hasher(value);
                break;
            }
            default:
                throw std::runtime_error("Unsupported field type for hashing.");
        }

        return hash;
    }
};

class ScanOperator : public Operator {
private:
    BufferManager& bufferManager;
    size_t currentPageIndex = 0;
    size_t currentSlotIndex = 0;
    std::unique_ptr<Tuple> currentTuple;
    size_t tuple_count = 0;
    std::string scan_relation = "";

public:
    ScanOperator(BufferManager& manager) : bufferManager(manager) {}

    ScanOperator(BufferManager& manager, std::string relation) : bufferManager(manager), scan_relation(relation) {}

    void open() override {
        currentPageIndex = 0;
        currentSlotIndex = 0;
        currentTuple.reset();
    }

    bool next() override {
        loadNextTuple();
        return currentTuple != nullptr;
    }

    void close() override {
        // std::cout << "print: " << tuple_count << "\n";
        currentPageIndex = 0;
        currentSlotIndex = 0;
        currentTuple.reset();
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        if (currentTuple) {
            if(scan_relation.length() != 0){
                currentTuple->fields.pop_back();
            }
            return std::move(currentTuple->fields);
        }
        return {}; // Return an empty vector if no tuple is available
    }

private:
    void loadNextTuple() {
        while (currentPageIndex < bufferManager.getNumPages()) {
            auto& currentPage = bufferManager.getPage(currentPageIndex);
            if (!currentPage || currentSlotIndex >= MAX_SLOTS) {
                currentSlotIndex = 0;
            }

            char* page_buffer = currentPage->page_data.get();
            Slot* slot_array = reinterpret_cast<Slot*>(page_buffer);

            while (currentSlotIndex < MAX_SLOTS) {
                if (!slot_array[currentSlotIndex].empty) {
                    assert(slot_array[currentSlotIndex].offset != INVALID_VALUE);
                    const char* tuple_data = page_buffer + slot_array[currentSlotIndex].offset;
                    std::istringstream iss(std::string(tuple_data, slot_array[currentSlotIndex].length));
                    currentTuple = Tuple::deserialize(iss);
                    if(scan_relation.length() > 0
                        && currentTuple->fields.back()->asString() != scan_relation){
                            currentSlotIndex++;
                            continue;
                    }
                    currentSlotIndex++;
                    tuple_count++;
                    return;
                }
                currentSlotIndex++;
            }
            currentPageIndex++;
        }
        currentTuple.reset();
    }
};

class IPredicate {
public:
    virtual ~IPredicate() = default;
    virtual bool check(const std::vector<std::unique_ptr<Field>>& tupleFields) const = 0;
};

void printTuple(const std::vector<std::unique_ptr<Field>>& tupleFields) {
    std::cout << "Tuple: [";
    for (const auto& field : tupleFields) {
        field->print();
        std::cout << " ";
    }
    std::cout << "]"<<std::endl;
}

class SimplePredicate: public IPredicate {
public:
    enum OperandType { DIRECT, INDIRECT };
    enum ComparisonOperator { EQ, NE, GT, GE, LT, LE };

    struct Operand {
        std::unique_ptr<Field> directValue;
        size_t index = 0;
        OperandType type;

        Operand(std::unique_ptr<Field> value) : directValue(std::move(value)), type(DIRECT) {}
        Operand(size_t idx) : index(idx), type(INDIRECT) {}
    };

    Operand left_operand;
    Operand right_operand;
    ComparisonOperator comparison_operator;

    SimplePredicate(Operand left, Operand right, ComparisonOperator op)
        : left_operand(std::move(left)), right_operand(std::move(right)), comparison_operator(op) {}

    bool check(const std::vector<std::unique_ptr<Field>>& tupleFields) const {
        const Field* leftField = nullptr;
        const Field* rightField = nullptr;

        if (left_operand.type == DIRECT) {
            leftField = left_operand.directValue.get();
        } else if (left_operand.type == INDIRECT) {
            leftField = tupleFields[left_operand.index].get();
        }

        if (right_operand.type == DIRECT) {
            rightField = right_operand.directValue.get();
        } else if (right_operand.type == INDIRECT) {
            rightField = tupleFields[right_operand.index].get();
        }

        if (leftField == nullptr || rightField == nullptr) {
            std::cerr << "error\n";
            return false;
        }

        if (leftField->getType() != rightField->getType()) {
            std::cerr << "error\n";
            return false;
        }

        switch (leftField->getType()) {
            case FieldType::INT: {
                int left_val = leftField->asInt();
                int right_val = rightField->asInt();
                return compare(left_val, right_val);
            }
            case FieldType::FLOAT: {
                float left_val = leftField->asFloat();
                float right_val = rightField->asFloat();
                return compare(left_val, right_val);
            }
            case FieldType::STRING: {
                std::string left_val = leftField->asString();
                std::string right_val = rightField->asString();
                return compare(left_val, right_val);
            }
            default:
                std::cerr << "error\n";
                return false;
        }
    }


private:
    template<typename T>
    bool compare(const T& left_val, const T& right_val) const {
        switch (comparison_operator) {
            case ComparisonOperator::EQ: return left_val == right_val;
            case ComparisonOperator::NE: return left_val != right_val;
            case ComparisonOperator::GT: return left_val > right_val;
            case ComparisonOperator::GE: return left_val >= right_val;
            case ComparisonOperator::LT: return left_val < right_val;
            case ComparisonOperator::LE: return left_val <= right_val;
            default: std::cerr << "error\n"; return false;
        }
    }
};

class ComplexPredicate : public IPredicate {
public:
    enum LogicOperator { AND, OR };

private:
    std::vector<std::unique_ptr<IPredicate>> predicates;
    LogicOperator logic_operator;

public:
    ComplexPredicate(LogicOperator op) : logic_operator(op) {}

    void addPredicate(std::unique_ptr<IPredicate> predicate) {
        predicates.push_back(std::move(predicate));
    }

    bool check(const std::vector<std::unique_ptr<Field>>& tupleFields) const {
        
        if (logic_operator == AND) {
            for (const auto& pred : predicates) {
                if (!pred->check(tupleFields)) {
                    return false;
                }
            }
            return true;
        } else if (logic_operator == OR) {
            for (const auto& pred : predicates) {
                if (pred->check(tupleFields)) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }


};


class SelectOperator : public UnaryOperator {
private:
    std::unique_ptr<IPredicate> predicate;
    bool has_next;
    std::vector<std::unique_ptr<Field>> currentOutput;

public:
    SelectOperator(Operator& input, std::unique_ptr<IPredicate> predicate)
        : UnaryOperator(input), predicate(std::move(predicate)), has_next(false) {}

    void open() override {
        input->open();
        has_next = false;
        currentOutput.clear();
    }

    bool next() override {
        while (input->next()) {
            const auto& output = input->getOutput();
            if (predicate->check(output)) {
                currentOutput.clear();
                for (const auto& field : output) {
                    currentOutput.push_back(field->clone());
                }
                has_next = true;
                return true;
            }
        }
        has_next = false;
        currentOutput.clear();
        return false;
    }

    void close() override {
        input->close();
        currentOutput.clear();
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        if (has_next) {
            std::vector<std::unique_ptr<Field>> outputCopy;
            for (const auto& field : currentOutput) {
                outputCopy.push_back(field->clone());
            }
            return outputCopy;
        } else {
            return {};
        }
    }
};
enum class AggrFuncType { COUNT, MAX, MIN, SUM };

struct AggrFunc {
    AggrFuncType func;
    size_t attr_index;
};

bool operator!=(const Field& lhs, const Field& rhs) {
    // TODO: Add your implementation here
    // UNUSED(lhs);
    // UNUSED(rhs);
    // return false;
    return !(lhs == rhs);
}

bool operator<(const Field& lhs, const Field& rhs) {
    // TODO: Add your implementation here
    // UNUSED(lhs);
    // UNUSED(rhs);
    // return false;
     if (lhs.type != rhs.type) {
        throw std::runtime_error("error");
    }

    switch (lhs.type) {
        case INT: {
            int left_val = *reinterpret_cast<const int*>(lhs.data.get());
            int right_val = *reinterpret_cast<const int*>(rhs.data.get());
            return left_val < right_val;
        }
        case FLOAT: {
            float left_val = *reinterpret_cast<const float*>(lhs.data.get());
            float right_val = *reinterpret_cast<const float*>(rhs.data.get());
            return left_val < right_val;
        }
        case STRING: {
            std::string left_val(lhs.data.get(), lhs.data_length - 1);
            std::string right_val(rhs.data.get(), rhs.data_length - 1);
            return left_val < right_val;
        }
        default:
            throw std::runtime_error("error");
    }
}

bool operator>(const Field& lhs, const Field& rhs) {
    // TODO: Add your implementation here
    // UNUSED(lhs);
    // UNUSED(rhs);
    // return false;
    return rhs < lhs;
}

bool operator<=(const Field& lhs, const Field& rhs) {
    // TODO: Add your implementation here
    // UNUSED(lhs);
    // UNUSED(rhs);
    // return false;
    return (lhs < rhs) || (lhs == rhs);
}

bool operator>=(const Field& lhs, const Field& rhs) {
    // TODO: Add your implementation here
    // UNUSED(lhs);
    // UNUSED(rhs);
    // return false;
    return (rhs <= lhs);
}

class PrintOperator : public UnaryOperator {
private:
    // TODO: Add your implementation here
    std::ostream& output_stream;
    std::vector<std::unique_ptr<Field>> current_tuple;
public:
    PrintOperator(Operator& input, std::ostream& stream) 
        : UnaryOperator(input), output_stream(stream) {
            // TODO: Add your implementation here
            // UNUSED(stream);
        }

    void open() override {
        // TODO: Add your implementation here
        input->open();
    }

    bool next() override {
        // TODO: Add your implementation here
        // return false;
        if (!input->next()) {
            return false;
        }
        current_tuple = input->getOutput();
        
        std::cout << "output: ";
        for (size_t i = 0; i < current_tuple.size(); ++i) {
            if (i > 0) {
                output_stream << ", ";
            }
            output_stream << current_tuple[i]->asString();
        }
        output_stream << "\n";
        
        return true;
    }

    void close() override {
        // TODO: Add your implementation here
        input->close();
        current_tuple.clear();
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        return {};
    }
};

class ProjectOperator : public UnaryOperator {
    private:
        // TODO: Add your implementation here
        std::vector<size_t> attribute_indexes;
        std::vector<std::unique_ptr<Field>> current_tuple;

    public:
        ProjectOperator(Operator& input, std::vector<size_t> attr_indexes)
            : UnaryOperator(input), attribute_indexes(std::move(attr_indexes)) {
                // TODO: Add your implementation here
                // UNUSED(attr_indexes);
            }

        ~ProjectOperator() = default;

        void open() override {
            // TODO: Add your implementation here
            input->open();
            current_tuple.clear();
        }

        bool next() override {
            // TODO: Add your implementation here
            // return false;
            if (!input->next()) {
                return false;
            }
            auto input_tuple = input->getOutput();
            current_tuple.clear();

            for (size_t index : attribute_indexes) {
                if (index < input_tuple.size()) {
                    current_tuple.push_back(input_tuple[index]->clone());
                } else {
                    throw std::runtime_error("error");
                }
            }

            return true;
        }

        void close() override {
            // TODO: Add your implementation here
            input->close();
            current_tuple.clear();
        }

        std::vector<std::unique_ptr<Field>> getOutput() override {
            // TODO: Add your implementation here
            // return {};
            std::vector<std::unique_ptr<Field>> result;
            for (const auto& field : current_tuple) {
                result.push_back(field->clone());
            }
            return result;
        }
};

class Sort : public UnaryOperator {
public:
    struct Criterion {
        size_t attr_index;
        bool desc;
    };

private:
    // TODO: Add your implementation here
    std::vector<Criterion> sort_criteria;
    std::vector<std::vector<std::unique_ptr<Field>>> all_tuples;
    size_t current_pos;

public:
    Sort(Operator& input, std::vector<Criterion> criteria)
        : UnaryOperator(input), sort_criteria(std::move(criteria)), current_pos(0) {
            // TODO: Add your implementation here
            // UNUSED(criteria);
        }

    ~Sort() override = default;

    void open() override {
        // TODO: Add your implementation here
        input->open();
        all_tuples.clear();
        current_pos = 0;

        while (input->next()) {
            all_tuples.push_back(std::move(input->getOutput()));
        }

        std::sort(all_tuples.begin(), all_tuples.end(), 
            [this](const auto& tuple1, const auto& tuple2) {
                for (const auto& criterion : sort_criteria) {
                    const Field& field1 = *tuple1[criterion.attr_index];
                    const Field& field2 = *tuple2[criterion.attr_index];
                    
                    if (field1 == field2) {
                        continue;
                    }
                    return criterion.desc ? field1 > field2 : field1 < field2;
                }
                return false;
            });
    }

    bool next() override {
        // TODO: Add your implementation here
        // return false;
        if (current_pos < all_tuples.size()) {
            current_pos++;
            return true;
        }
        return false;
    }

    void close() override {
        // TODO: Add your implementation here
        input->close();
        all_tuples.clear();
        current_pos = 0;
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        // TODO: Add your implementation here
        // return {};
        if (current_pos > 0 && current_pos <= all_tuples.size()) {
            std::vector<std::unique_ptr<Field>> result;
            for (const auto& field : all_tuples[current_pos - 1]) {
                result.push_back(field->clone());
            }
            return result;
        }
        return {};
    }
};

/// Computes the inner equi-join of the two inputs on one attribute.
class HashJoin : public BinaryOperator {
private:
    // TODO: Add your implementation here
    size_t left_index;
    size_t right_index;
    std::unordered_multimap<Field, std::vector<std::unique_ptr<Field>>, FieldHasher> hash_table;
    std::vector<std::vector<std::unique_ptr<Field>>> current_matches;
    size_t current_match_index;
    std::vector<std::unique_ptr<Field>> right_tuple;
    bool hash_table_built;

public:
    HashJoin(Operator& left_input, Operator& right_input, size_t left_attr_index, size_t right_attr_index)
        : BinaryOperator(left_input, right_input), left_index(left_attr_index), right_index(right_attr_index),
          current_match_index(0), hash_table_built(false) {
            // TODO: Add your implementation here
            // UNUSED(left_attr_index);
            // UNUSED(right_attr_index);
        }

    ~HashJoin() override = default;

    void open() override {
        // TODO: Add your implementation here
        input_left->open();
        input_right->open();
        hash_table.clear();
        current_matches.clear();
        current_match_index = 0;
        hash_table_built = false;
    }

    bool next() override {
        // TODO: Add your implementation here
        // return false;
        if (!hash_table_built) {
            while (input_left->next()) {
                auto tuple = input_left->getOutput();
                if (left_index < tuple.size()) {
                    auto key = *tuple[left_index];
                    std::vector<std::unique_ptr<Field>> stored_tuple;
                    for (const auto& field : tuple) {
                        stored_tuple.push_back(field->clone());
                    }
                    hash_table.emplace(key, std::move(stored_tuple));
                }
            }
            hash_table_built = true;
        }

        if (current_match_index < current_matches.size()) {
            current_match_index++;
            return true;
        }

        while (input_right->next()) {
            right_tuple = input_right->getOutput();
            if (right_index < right_tuple.size()) {
                auto probe_key = *right_tuple[right_index];
                
                current_matches.clear();
                current_match_index = 0;
                
                auto range = hash_table.equal_range(probe_key);
                for (auto it = range.first; it != range.second; ++it) {
                    std::vector<std::unique_ptr<Field>> joined_tuple;
                    for (const auto& field : it->second) {
                        joined_tuple.push_back(field->clone());
                    }
                    for (const auto& field : right_tuple) {
                        joined_tuple.push_back(field->clone());
                    }
                    current_matches.push_back(std::move(joined_tuple));
                }
                if (!current_matches.empty()) {
                    current_match_index = 1;
                    return true;
                }
            }
        }
        
        return false;
    }

    void close() override {
        // TODO: Add your implementation here
        input_left->close();
        input_right->close();
        hash_table.clear();
        current_matches.clear();
        right_tuple.clear();
        current_match_index = 0;
        hash_table_built = false;
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        // TODO: Add your implementation here
        // return {};
        if (current_match_index > 0 && current_match_index <= current_matches.size()) {
            std::vector<std::unique_ptr<Field>> result;
            for (const auto& field : current_matches[current_match_index - 1]) {
                result.push_back(field->clone());
            }
            return result;
        }
        return {};
    }
};

/// Groups and calculates (potentially multiple) aggregates on the input.
class HashAggregationOperator : public UnaryOperator {
private:
    // TODO: Add your implementation here
    std::vector<size_t> group_by_attributes;
    std::vector<AggrFunc> aggregate_functions;
    
    struct AggregateValues {
        std::vector<int> counts;
        std::vector<int> sums;
        std::vector<int> mins;
        std::vector<int> maxs;
        std::vector<std::unique_ptr<Field>> group_by_fields;
        std::vector<std::string> mins_str;
        std::vector<std::string> maxs_str; 
    };
    
    std::unordered_map<std::vector<Field>, AggregateValues, FieldVectorHasher> groups;
    std::vector<std::vector<std::unique_ptr<Field>>> result_tuples;
    size_t current_tuple_index;
    bool aggregation_done;

public:
    HashAggregationOperator(Operator& input, std::vector<size_t> group_by_attrs, std::vector<AggrFunc> aggr_funcs)
        : UnaryOperator(input), group_by_attributes(std::move(group_by_attrs)), aggregate_functions(std::move(aggr_funcs)),
          current_tuple_index(0), aggregation_done(false) {
            // TODO: Add your implementation here
            // UNUSED(group_by_attrs);
            // UNUSED(aggr_funcs);
        }

    void open() override {
        // TODO: Add your implementation here
        input->open();
        groups.clear();
        result_tuples.clear();
        current_tuple_index = 0;
        aggregation_done = false;
    }

    bool next() override {
        // TODO: Add your implementation here
        // return false;
        if (!aggregation_done) {
            computeAggregations();
            aggregation_done = true;
        }

        if (current_tuple_index < result_tuples.size()) {
            current_tuple_index++;
            return true;
        }
        return false;
    }

    void close() override {
        // TODO: Add your implementation here
        input->close();
        groups.clear();
        result_tuples.clear();
        current_tuple_index = 0;
        aggregation_done = false;
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        // TODO: Add your implementation here
        // return {};
        if (current_tuple_index > 0 && current_tuple_index <= result_tuples.size()) {
            std::vector<std::unique_ptr<Field>> result;
            for (const auto& field : result_tuples[current_tuple_index - 1]) {
                result.push_back(field->clone());
            }
            return result;
        }
        return {};
    }

private:
    void computeAggregations() {
        while (input->next()) {
            auto tuple = input->getOutput();
            std::vector<Field> group_key;
            std::vector<std::unique_ptr<Field>> group_fields;
            for (size_t idx : group_by_attributes) {
                if (idx < tuple.size()) {
                    group_key.push_back(*tuple[idx]);
                    group_fields.push_back(tuple[idx]->clone());
                }
            }

            auto& group = groups[group_key];
            if (group.group_by_fields.empty()) {
                group.group_by_fields = std::move(group_fields);
                group.counts.resize(aggregate_functions.size(), 0);
                group.sums.resize(aggregate_functions.size(), 0);
                group.mins.resize(aggregate_functions.size(), std::numeric_limits<int>::max());
                group.maxs.resize(aggregate_functions.size(), std::numeric_limits<int>::min());
                group.mins_str.resize(aggregate_functions.size(), std::string("\xFF\xFF\xFF\xFF"));
                group.maxs_str.resize(aggregate_functions.size(), "");
            }

            for (size_t i = 0; i < aggregate_functions.size(); ++i) {
                const auto& func = aggregate_functions[i];
                if (func.attr_index < tuple.size() && tuple[func.attr_index]->getType() == INT) {
                    int value = tuple[func.attr_index]->asInt();
                    
                    switch (func.func) {
                        case AggrFuncType::COUNT:
                            group.counts[i]++;
                            break;
                        case AggrFuncType::SUM:
                            group.sums[i] += value;
                            break;
                        case AggrFuncType::MIN:
                            group.mins[i] = std::min(group.mins[i], value);
                            break;
                        case AggrFuncType::MAX:
                            group.maxs[i] = std::max(group.maxs[i], value);
                            break;
                    }
                } else if (tuple[func.attr_index]->getType() == STRING) {
                    std::string value = tuple[func.attr_index]->asString();
                    switch (func.func) {
                        case AggrFuncType::MIN:
                            if (value < group.mins_str[i]) {
                                group.mins_str[i] = value;
                            }
                            break;
                        case AggrFuncType::MAX:
                            if (value > group.maxs_str[i]) {
                                group.maxs_str[i] = value;
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        for (const auto& group_entry : groups) {
            std::vector<std::unique_ptr<Field>> result_tuple;
            
            for (const auto& field : group_entry.second.group_by_fields) {
                result_tuple.push_back(field->clone());
            }
            for (size_t i = 0; i < aggregate_functions.size(); ++i) {
                switch (aggregate_functions[i].func) {
                    case AggrFuncType::COUNT:
                        result_tuple.push_back(std::make_unique<Field>(group_entry.second.counts[i]));
                        break;
                    case AggrFuncType::SUM:
                        result_tuple.push_back(std::make_unique<Field>(group_entry.second.sums[i]));
                        break;
                    case AggrFuncType::MIN:
                        if (aggregate_functions[i].attr_index == 1) {
                            result_tuple.push_back(std::make_unique<Field>(group_entry.second.mins_str[i]));
                        } else {
                            result_tuple.push_back(std::make_unique<Field>(group_entry.second.mins[i]));
                        }
                        break;
                    case AggrFuncType::MAX:
                        if (aggregate_functions[i].attr_index == 1) {
                            result_tuple.push_back(std::make_unique<Field>(group_entry.second.maxs_str[i]));
                        } else {
                            result_tuple.push_back(std::make_unique<Field>(group_entry.second.maxs[i]));
                        }
                        break;
                }
            }
            
            result_tuples.push_back(std::move(result_tuple));
        }
    }
};

/// Computes the union of the two inputs with set semantics.
class UnionOperator : public BinaryOperator {
    private:
        // TODO: Add your implementation here
        std::unordered_set<std::vector<Field>, FieldVectorHasher> unique_tuples;
        std::vector<std::vector<std::unique_ptr<Field>>> result_tuples;
        size_t current_tuple_index;
        bool input_processed;

    public:
        UnionOperator(Operator& left_input, Operator& right_input)
            : BinaryOperator(left_input, right_input), current_tuple_index(0), input_processed(false) {}

        ~UnionOperator() = default;

        void open() override {
            // TODO: Add your implementation here
            input_left->open();
            input_right->open();
            unique_tuples.clear();
            result_tuples.clear();
            current_tuple_index = 0;
            input_processed = false;
        }

        bool next() override {
            // TODO: Add your implementation here
            // return false;
            if (!input_processed) {
                while (input_left->next()) {
                    auto tuple = input_left->getOutput();
                    std::vector<Field> key;
                    for (const auto& field : tuple) {
                        key.push_back(*field);
                    }
                    if (unique_tuples.insert(key).second) {
                        result_tuples.push_back(std::move(tuple));
                    }
                }
                while (input_right->next()) {
                    auto tuple = input_right->getOutput();
                    std::vector<Field> key;
                    for (const auto& field : tuple) {
                        key.push_back(*field);
                    }
                    if (unique_tuples.insert(key).second) {
                        result_tuples.push_back(std::move(tuple));
                    }
                }

                input_processed = true;
            }

            if (current_tuple_index < result_tuples.size()) {
                current_tuple_index++;
                return true;
            }
            return false;
        }

        void close() override {
            // TODO: Add your implementation here
            input_left->close();
            input_right->close();
            unique_tuples.clear();
            result_tuples.clear();
            current_tuple_index = 0;
            input_processed = false;
        }

        std::vector<std::unique_ptr<Field>> getOutput() override {
            // TODO: Add your implementation here
            // return {};
            if (current_tuple_index > 0 && current_tuple_index <= result_tuples.size()) {
                std::vector<std::unique_ptr<Field>> result;
                for (const auto& field : result_tuples[current_tuple_index - 1]) {
                    result.push_back(field->clone());
                }
                return result;
            }
            return {};
        }

};

/// Computes the union of the two inputs with bag semantics.
class UnionAll: public BinaryOperator {
    private:
        // TODO: Add your implementation here
        bool processing_left;
        std::vector<std::unique_ptr<Field>> current_tuple;

    public:
        UnionAll(Operator& left_input, Operator& right_input)
            : BinaryOperator(left_input, right_input), processing_left(true) {}

        ~UnionAll() = default;

        void open() override {
            // TODO: Add your implementation here
            input_left->open();
            input_right->open();
            processing_left = true;
            current_tuple.clear();
        }

        bool next() override {
            // TODO: Add your implementation here
            // return false;
            if (processing_left) {
                if (input_left->next()) {
                    current_tuple = input_left->getOutput();
                    return true;
                }
                processing_left = false;
            }
            if (input_right->next()) {
                current_tuple = input_right->getOutput();
                return true;
            }
            
            return false;
        }

        void close() override {
            // TODO: Add your implementation here
            input_left->close();
            input_right->close();
            current_tuple.clear();
            processing_left = true;
        }

        std::vector<std::unique_ptr<Field>> getOutput() override {
            // TODO: Add your implementation here
            // return {};
            std::vector<std::unique_ptr<Field>> result;
            for (const auto& field : current_tuple) {
                result.push_back(field->clone());
            }
            return result;
        }
};

/// Computes the intersection of the two inputs with set semantics.
class Intersect: public BinaryOperator {
    private:
        // TODO: Add your implementation here
        std::unordered_set<std::vector<Field>, FieldVectorHasher> left_set;
        std::vector<std::vector<std::unique_ptr<Field>>> result_tuples;
        size_t current_tuple_index;
        bool input_processed;

    public:
        Intersect(Operator& left_input, Operator& right_input)
            : BinaryOperator(left_input, right_input), current_tuple_index(0), input_processed(false) {}

        ~Intersect() = default;

        void open() override {
            // TODO: Add your implementation here
            input_left->open();
            input_right->open();
            left_set.clear();
            result_tuples.clear();
            current_tuple_index = 0;
            input_processed = false;
        }

        bool next() override {
            // TODO: Add your implementation here
            // return false;
            if (!input_processed) {
                while (input_left->next()) {
                    auto tuple = input_left->getOutput();
                    std::vector<Field> key;
                    for (const auto& field : tuple) {
                        key.push_back(*field);
                    }
                    left_set.insert(key);
                }

                // Then process right input and keep matches
                while (input_right->next()) {
                    auto tuple = input_right->getOutput();
                    std::vector<Field> key;
                    for (const auto& field : tuple) {
                        key.push_back(*field);
                    }
                    if (left_set.find(key) != left_set.end()) {
                        bool already_in_results = false;
                        for (const auto& result_tuple : result_tuples) {
                            bool match = true;
                            for (size_t i = 0; i < result_tuple.size(); i++) {
                                if (!(*result_tuple[i] == *tuple[i])) {
                                    match = false;
                                    break;
                                }
                            }
                            if (match) {
                                already_in_results = true;
                                break;
                            }
                        }
                        
                        if (!already_in_results) {
                            result_tuples.push_back(std::move(tuple));
                        }
                    }
                }

                input_processed = true;
            }

            if (current_tuple_index < result_tuples.size()) {
                current_tuple_index++;
                return true;
            }
            return false;
        }

        void close() override {
            // TODO: Add your implementation here
            input_left->close();
            input_right->close();
            left_set.clear();
            result_tuples.clear();
            current_tuple_index = 0;
            input_processed = false;
        }

        std::vector<std::unique_ptr<Field>> getOutput() override {
            // TODO: Add your implementation here
            // return {};
            if (current_tuple_index > 0 && current_tuple_index <= result_tuples.size()) {
                std::vector<std::unique_ptr<Field>> result;
                for (const auto& field : result_tuples[current_tuple_index - 1]) {
                    result.push_back(field->clone());
                }
                return result;
            }
            return {};
        }
};

/// Computes the intersection of the two inputs with bag semantics.
class IntersectAll: public BinaryOperator {
    private:
        // TODO: Add your implementation here
        std::unordered_map<std::vector<Field>, std::pair<int, int>, FieldVectorHasher> tuple_counts;
        std::vector<std::vector<std::unique_ptr<Field>>> result_tuples;
        size_t current_tuple_index;
        bool input_processed;

    public:
        IntersectAll(Operator& left_input, Operator& right_input)
            : BinaryOperator(left_input, right_input), current_tuple_index(0), input_processed(false) {}

        ~IntersectAll() = default;

        void open() override {
            // TODO: Add your implementation here
            input_left->open();
            input_right->open();
            tuple_counts.clear();
            result_tuples.clear();
            current_tuple_index = 0;
            input_processed = false;
        }

        bool next() override {
            // TODO: Add your implementation here
            // return false;
            if (!input_processed) {
                while (input_left->next()) {
                    auto tuple = input_left->getOutput();
                    std::vector<Field> key;
                    for (const auto& field : tuple) {
                        key.push_back(*field);
                    }
                    tuple_counts[key].first++;
                }
                while (input_right->next()) {
                    auto tuple = input_right->getOutput();
                    std::vector<Field> key;
                    for (const auto& field : tuple) {
                        key.push_back(*field);
                    }
                    tuple_counts[key].second++;
                }

                for (const auto& entry : tuple_counts) {
                    int min_count = std::min(entry.second.first, entry.second.second);
                    if (min_count > 0) {
                        for (int i = 0; i < min_count; i++) {
                            std::vector<std::unique_ptr<Field>> result_tuple;
                            for (const auto& field : entry.first) {
                                result_tuple.push_back(field.clone());
                            }
                            result_tuples.push_back(std::move(result_tuple));
                        }
                    }
                }

                input_processed = true;
            }

            if (current_tuple_index < result_tuples.size()) {
                current_tuple_index++;
                return true;
            }
            return false;
        }

        void close() override {
            // TODO: Add your implementation here
            input_left->close();
            input_right->close();
            tuple_counts.clear();
            result_tuples.clear();
            current_tuple_index = 0;
            input_processed = false;
        }

        std::vector<std::unique_ptr<Field>> getOutput() override {
            // TODO: Add your implementation here
            // return {};
            if (current_tuple_index > 0 && current_tuple_index <= result_tuples.size()) {
                std::vector<std::unique_ptr<Field>> result;
                for (const auto& field : result_tuples[current_tuple_index - 1]) {
                    result.push_back(field->clone());
                }
                return result;
            }
            return {};
        }
};

/// Computes input_left - input_right with set semantics.
class Except: public BinaryOperator {
    private:
        // TODO: Add your implementation here
        std::unordered_set<std::vector<Field>, FieldVectorHasher> right_set;
        std::vector<std::vector<std::unique_ptr<Field>>> result_tuples;
        size_t current_tuple_index;
        bool input_processed;

    public:
        Except(Operator& left_input, Operator& right_input)
            : BinaryOperator(left_input, right_input), current_tuple_index(0), input_processed(false) {}

        ~Except() = default;

        void open() override {
            // TODO: Add your implementation here
            input_left->open();
            input_right->open();
            right_set.clear();
            result_tuples.clear();
            current_tuple_index = 0;
            input_processed = false;
        }

        bool next() override {
            // TODO: Add your implementation here
            // return false;
            if (!input_processed) {
                while (input_right->next()) {
                    auto tuple = input_right->getOutput();
                    std::vector<Field> key;
                    for (const auto& field : tuple) {
                        key.push_back(*field);
                    }
                    right_set.insert(key);
                }

                while (input_left->next()) {
                    auto tuple = input_left->getOutput();
                    std::vector<Field> key;
                    for (const auto& field : tuple) {
                        key.push_back(*field);
                    }
                    if (right_set.find(key) == right_set.end()) {
                        bool already_in_results = false;
                        for (const auto& result_tuple : result_tuples) {
                            bool match = true;
                            for (size_t i = 0; i < result_tuple.size(); i++) {
                                if (!(*result_tuple[i] == *tuple[i])) {
                                    match = false;
                                    break;
                                }
                            }
                            if (match) {
                                already_in_results = true;
                                break;
                            }
                        }
                        
                        if (!already_in_results) {
                            result_tuples.push_back(std::move(tuple));
                        }
                    }
                }

                input_processed = true;
            }

            if (current_tuple_index < result_tuples.size()) {
                current_tuple_index++;
                return true;
            }
            return false;
        }

        void close() override {
            // TODO: Add your implementation here
            input_left->close();
            input_right->close();
            right_set.clear();
            result_tuples.clear();
            current_tuple_index = 0;
            input_processed = false;
        }

        std::vector<std::unique_ptr<Field>> getOutput() override {
            // TODO: Add your implementation here
            // return {};
            if (current_tuple_index > 0 && current_tuple_index <= result_tuples.size()) {
                std::vector<std::unique_ptr<Field>> result;
                for (const auto& field : result_tuples[current_tuple_index - 1]) {
                    result.push_back(field->clone());
                }
                return result;
            }
            return {};
        }
};

/// Computes input_left - input_right with bag semantics.
class ExceptAll: public BinaryOperator {
    private:
        // TODO: Add your implementation here
        std::unordered_map<std::vector<Field>, std::pair<int, int>, FieldVectorHasher> tuple_counts;
        std::vector<std::vector<std::unique_ptr<Field>>> result_tuples;
        size_t current_tuple_index;
        bool input_processed;

    public:
        ExceptAll(Operator& left_input, Operator& right_input)
            : BinaryOperator(left_input, right_input), current_tuple_index(0), input_processed(false) {}

        ~ExceptAll() = default;

        void open() override {
            // TODO: Add your implementation here
            input_left->open();
            input_right->open();
            tuple_counts.clear();
            result_tuples.clear();
            current_tuple_index = 0;
            input_processed = false;
        }

        bool next() override {
            // TODO: Add your implementation here
            // return false;
            if (!input_processed) {
                while (input_left->next()) {
                    auto tuple = input_left->getOutput();
                    std::vector<Field> key;
                    for (const auto& field : tuple) {
                        key.push_back(*field);
                    }
                    tuple_counts[key].first++;
                }
                while (input_right->next()) {
                    auto tuple = input_right->getOutput();
                    std::vector<Field> key;
                    for (const auto& field : tuple) {
                        key.push_back(*field);
                    }
                    tuple_counts[key].second++;
                }

                for (const auto& entry : tuple_counts) {
                    int diff_count = entry.second.first - entry.second.second;
                    if (diff_count > 0) {
                        for (int i = 0; i < diff_count; i++) {
                            std::vector<std::unique_ptr<Field>> result_tuple;
                            for (const auto& field : entry.first) {
                                result_tuple.push_back(field.clone());
                            }
                            result_tuples.push_back(std::move(result_tuple));
                        }
                    }
                }

                input_processed = true;
            }

            if (current_tuple_index < result_tuples.size()) {
                current_tuple_index++;
                return true;
            }
            return false;
        }

        void close() override {
            // TODO: Add your implementation here
            input_left->close();
            input_right->close();
            tuple_counts.clear();
            result_tuples.clear();
            current_tuple_index = 0;
            input_processed = false;
        }

        std::vector<std::unique_ptr<Field>> getOutput() override {
            // TODO: Add your implementation here
            // return {};
            if (current_tuple_index > 0 && current_tuple_index <= result_tuples.size()) {
                std::vector<std::unique_ptr<Field>> result;
                for (const auto& field : result_tuples[current_tuple_index - 1]) {
                    result.push_back(field->clone());
                }
                return result;
            }
            return {};
        }
};

class InsertOperator : public Operator {
private:
    BufferManager& bufferManager;
    std::unique_ptr<Tuple> tupleToInsert;

public:
    InsertOperator(BufferManager& manager) : bufferManager(manager) {}

    // Set the tuple to be inserted by this operator.
    void setTupleToInsert(std::unique_ptr<Tuple> tuple) {
        tupleToInsert = std::move(tuple);
    }

    void open() override {
        // Not used in this context
    }

    bool next() override {
        if (!tupleToInsert) return false; // No tuple to insert

        for (size_t pageId = 0; pageId < bufferManager.getNumPages(); ++pageId) {
            auto& page = bufferManager.getPage(pageId);
            // Attempt to insert the tuple
            if (page->addTuple(tupleToInsert->clone())) { 
                // Flush the page to disk after insertion
                bufferManager.flushPage(pageId); 
                return true; // Insertion successful
            }
        }

        // If insertion failed in all existing pages, extend the database and try again
        bufferManager.extend();
        auto& newPage = bufferManager.getPage(bufferManager.getNumPages() - 1);
        if (newPage->addTuple(tupleToInsert->clone())) {
            bufferManager.flushPage(bufferManager.getNumPages() - 1);
            return true; // Insertion successful after extending the database
        }

        return false; // Insertion failed even after extending the database
    }

    void close() override {
        // Not used in this context
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        return {}; // Return empty vector
    }
};

class DeleteOperator : public Operator {
private:
    BufferManager& bufferManager;
    size_t pageId;
    size_t tupleId;

public:
    DeleteOperator(BufferManager& manager, size_t pageId, size_t tupleId) 
        : bufferManager(manager), pageId(pageId), tupleId(tupleId) {}

    void open() override {
        // Not used in this context
    }

    bool next() override {
        auto& page = bufferManager.getPage(pageId);
        if (!page) {
            std::cerr << "Page not found." << std::endl;
            return false;
        }

        page->deleteTuple(tupleId); // Perform deletion
        bufferManager.flushPage(pageId); // Flush the page to disk after deletion
        return true;
    }

    void close() override {
        // Not used in this context
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        return {}; // Return empty vector
    }
};


// Query Executor functions
struct QueryComponents {
    std::vector<size_t> selectAttributes;
    bool sumOperation = false;
    int sumAttributeIndex = -1;
    bool groupBy = false;
    int groupByAttributeIndex = -1;
    bool whereCondition = false;
    int whereAttributeIndex = -1;
    int lowerBound = std::numeric_limits<int>::min();
    int upperBound = std::numeric_limits<int>::max();

    bool innerJoin = false;
    int joinAttributeIndex1 = -1;
    int joinAttributeIndex2 = -1;
    std::string relation;
    std::string joinRelation;
};

QueryComponents parseQuery(const std::string& query) {
    QueryComponents components;

    // Parse selected attributes
    std::regex selectRegex("SELECT \\{(\\d+)\\}(, \\{(\\d+)\\})?");
    std::smatch selectMatches;
    if (std::regex_search(query, selectMatches, selectRegex)) {
        for (size_t i = 1; i < selectMatches.size(); ++i) {
            if (!selectMatches[i].str().empty() && selectMatches[i] != "*") {
                components.selectAttributes.push_back(std::stoi(selectMatches[i]) - 1);
            }
        }
    }





    // TODO: Add your implementation here for parsing 'FROM'
    std::regex fromRegex("FROM \\{([^}]+)\\}");
    std::smatch fromMatches;
    if (std::regex_search(query, fromMatches, fromRegex)) {
        components.relation = fromMatches[1].str();
    }

    // TODO: Add your implementation here for parsing 'JOIN'
    std::regex joinRegex("JOIN \\{([^}]+)\\} ON \\{(\\d+)\\} = \\{(\\d+)\\}");
    std::smatch joinMatches;
    if (std::regex_search(query, joinMatches, joinRegex)) {
        components.innerJoin = true;
        components.joinRelation = joinMatches[1].str();
        components.joinAttributeIndex1 = std::stoi(joinMatches[2]) - 1;
        components.joinAttributeIndex2 = std::stoi(joinMatches[3]) - 1;
    }








    // Check for SUM operation
    std::regex sumRegex("SUM\\{(\\d+)\\}");
    std::smatch sumMatches;
    if (std::regex_search(query, sumMatches, sumRegex)) {
        components.sumOperation = true;
        components.sumAttributeIndex = std::stoi(sumMatches[1]) - 1;
    }

    // Check for GROUP BY clause
    std::regex groupByRegex("GROUP BY \\{(\\d+)\\}");
    std::smatch groupByMatches;
    if (std::regex_search(query, groupByMatches, groupByRegex)) {
        components.groupBy = true;
        components.groupByAttributeIndex = std::stoi(groupByMatches[1]) - 1;
    }

    // Extract WHERE conditions more accurately
    std::regex whereRegex("\\{(\\d+)\\} > (\\d+) and \\{(\\d+)\\} < (\\d+)");
    std::smatch whereMatches;
    if (std::regex_search(query, whereMatches, whereRegex)) {
        components.whereCondition = true;
        // Correctly identify the attribute index for the WHERE condition
        components.whereAttributeIndex = std::stoi(whereMatches[1]) - 1;
        components.lowerBound = std::stoi(whereMatches[2]);
        // Ensure the same attribute is used for both conditions
        if (std::stoi(whereMatches[3]) - 1 == components.whereAttributeIndex) {
            components.upperBound = std::stoi(whereMatches[4]);
        } else {
            std::cerr << "Error: WHERE clause conditions apply to different attributes." << std::endl;
            // Handle error or set components.whereCondition = false;
        }
    }

    return components;
}

void prettyPrint(const QueryComponents& components) {
    std::cout << "Query Components:\n";
    std::cout << "  Selected Attributes: ";
    for (auto attr : components.selectAttributes) {
        std::cout << "{" << attr + 1 << "} "; // Convert back to 1-based indexing for display
    }
    std::cout << "\n  SUM Operation: " << (components.sumOperation ? "Yes" : "No");
    if (components.sumOperation) {
        std::cout << " on {" << components.sumAttributeIndex + 1 << "}";
    }
    std::cout << "\n  GROUP BY: " << (components.groupBy ? "Yes" : "No");
    if (components.groupBy) {
        std::cout << " on {" << components.groupByAttributeIndex + 1 << "}";
    }
    std::cout << "\n  WHERE Condition: " << (components.whereCondition ? "Yes" : "No");
    if (components.whereCondition) {
        std::cout << " on {" << components.whereAttributeIndex + 1 << "} > " << components.lowerBound << " and < " << components.upperBound;
    }
    std::cout << std::endl;
}

std::vector<std::vector<std::unique_ptr<Field>>> executeQuery(const QueryComponents& components, 
                  BufferManager& buffer_manager) {
    // Stack allocation of ScanOperator
    ScanOperator scanOp(buffer_manager, components.relation);
    ScanOperator scanOp2(buffer_manager, components.joinRelation);

    // Using a pointer to Operator to handle polymorphism
    Operator* rootOp = &scanOp;

    // Buffer for optional operators to ensure lifetime
    std::optional<SelectOperator> selectOpBuffer;
    std::optional<HashAggregationOperator> hashAggOpBuffer;
    std::optional<HashJoin> hashJoinBuffer;







    // TODO: Add your implementation here for handling JOIN
    if (components.innerJoin) {
        hashJoinBuffer.emplace(scanOp, scanOp2, 
                            components.joinAttributeIndex1,
                            components.joinAttributeIndex2);
        rootOp = &*hashJoinBuffer;
    }









    // Apply WHERE conditions
    if (components.whereAttributeIndex != -1) {
        // Create simple predicates with comparison operators
        auto predicate1 = std::make_unique<SimplePredicate>(
            SimplePredicate::Operand(components.whereAttributeIndex),
            SimplePredicate::Operand(std::make_unique<Field>(components.lowerBound)),
            SimplePredicate::ComparisonOperator::GT
        );

        auto predicate2 = std::make_unique<SimplePredicate>(
            SimplePredicate::Operand(components.whereAttributeIndex),
            SimplePredicate::Operand(std::make_unique<Field>(components.upperBound)),
            SimplePredicate::ComparisonOperator::LT
        );

        // Combine simple predicates into a complex predicate with logical AND operator
        auto complexPredicate = std::make_unique<ComplexPredicate>(ComplexPredicate::LogicOperator::AND);
        complexPredicate->addPredicate(std::move(predicate1));
        complexPredicate->addPredicate(std::move(predicate2));

        // Using std::optional to manage the lifetime of SelectOperator
        selectOpBuffer.emplace(*rootOp, std::move(complexPredicate));
        rootOp = &*selectOpBuffer;
    }

    // Apply SUM or GROUP BY operation
    if (components.sumOperation || components.groupBy) {
        std::vector<size_t> groupByAttrs;
        if (components.groupBy) {
            groupByAttrs.push_back(static_cast<size_t>(components.groupByAttributeIndex));
        }
        std::vector<AggrFunc> aggrFuncs{
            {AggrFuncType::SUM, static_cast<size_t>(components.sumAttributeIndex)}
        };

        // Using std::optional to manage the lifetime of HashAggregationOperator
        hashAggOpBuffer.emplace(*rootOp, groupByAttrs, aggrFuncs);
        rootOp = &*hashAggOpBuffer;
    }

    // Execute the Root Operator
    std::vector<std::vector<std::unique_ptr<Field>> > result;
    rootOp->open();
    while (rootOp->next()) {
        // Retrieve and print the current tuple
        const auto& output = rootOp->getOutput();
        std::vector<std::unique_ptr<Field>> tuple;
        for (const auto& field : output) {
            tuple.push_back(std::make_unique<Field>(*field));
        }
        result.push_back(std::move(tuple));
    }
    rootOp->close();
    return result;
}




// helper functions for running tests
enum class Relation {
    STUDENTS,
    GRADES,
    FEEDBACK,
};

const std::vector<std::tuple<int, std::string, int>> relation_students{
    {24002, "Xenokrates", 24},
    {26120, "Fichte", 26},
    {29555, "Feuerbach", 29},
    {28000, "Schopenhauer", 46},
    {24123, "Platon", 50},
    {25198, "Aristoteles", 50},
};

const std::vector<std::tuple<int, int, int>> relation_grades{
    {24002, 5001, 1},
    {24002, 5041, 2},
    {29555, 4630, 2},
    {26120, 5001, 3},
    {24123, 5001, 2},
    {25198, 4630, 1},
    {29555, 5041, 1}
};

const std::vector<std::tuple<int, std::string>> relation_feedback{
    {5001, "good"},
    {5041, "bad"},
    {4630, "average"},
    {5041, "good"},
    {4630, "average"},
    {4630, "good"}
};

std::string sutdentsRelationToString() {
    std::stringstream result;
    for(const auto& student : relation_students) {
        result << std::get<0>(student) << ", " << std::get<1>(student)
            << ", " << std::get<2>(student) << std::endl;
    }
    return result.str();
}

class TestTupleSource : public Operator {
    private:
        std::vector<std::vector<std::unique_ptr<Field>>> tuples;
        size_t current_tuple_index = 0;
        Relation type;

    public:
        bool opened = false;
        bool closed = false;

        TestTupleSource(Relation type) : type(type) {}

        void open() override {
            current_tuple_index = 0;
            opened = true;

            switch (type) {
                case Relation::STUDENTS:
                    for (const auto& student : relation_students) {
                        std::vector<std::unique_ptr<Field>> fields;
                        fields.push_back(std::make_unique<Field>(std::get<0>(student)));
                        fields.push_back(std::make_unique<Field>(std::get<1>(student)));
                        fields.push_back(std::make_unique<Field>(std::get<2>(student)));
                        tuples.push_back(std::move(fields));
                    }
                    break;
                case Relation::GRADES:
                    for (const auto& grade : relation_grades) {
                        std::vector<std::unique_ptr<Field>> fields;
                        fields.push_back(std::make_unique<Field>(std::get<0>(grade)));
                        fields.push_back(std::make_unique<Field>(std::get<1>(grade)));
                        fields.push_back(std::make_unique<Field>(std::get<2>(grade)));
                        tuples.push_back(std::move(fields));
                    }
                    break;
                case Relation::FEEDBACK:
                    for (const auto& feedback : relation_feedback) {
                        std::vector<std::unique_ptr<Field>> fields;
                        fields.push_back(std::make_unique<Field>(std::get<0>(feedback)));
                        fields.push_back(std::make_unique<Field>(std::get<1>(feedback)));
                        tuples.push_back(std::move(fields));
                    }
                    break;
            }
        }

        bool next() override {
            return current_tuple_index < tuples.size();
        }

        void close() override {
            tuples.clear();
            current_tuple_index = 0;
            closed = true;
            opened = false;
        }

        std::vector<std::unique_ptr<Field>> getOutput() override {
            if (current_tuple_index < tuples.size()) {
                std::vector<std::unique_ptr<Field>> result;
                for (const auto& field : tuples[current_tuple_index]) {
                    result.push_back(std::unique_ptr<Field>(field->clone()));
                }
                current_tuple_index++;
                return result;
            }
            return {};
        }
};

std::string sort_output(const std::string& str) {
    std::vector<std::string> lines;
    size_t str_pos = 0;
    while (true) {
        size_t new_str_pos = str.find('\n', str_pos);
        if (new_str_pos == std::string::npos) {
            lines.emplace_back(&str[str_pos], str.size() - str_pos);
            break;
        }
        lines.emplace_back(&str[str_pos], new_str_pos - str_pos + 1);
        str_pos = new_str_pos + 1;
        if (str_pos == str.size()) {
            break;
        }
    }
    std::sort(lines.begin(), lines.end());
    std::string sorted_str;
    for (auto& line : lines) {
        sorted_str.append(line);
    }
    return sorted_str;
}

void poplulate_db(Relation relation, BufferManager& buffer_manager) {
    InsertOperator insert_operator(buffer_manager);
    if(relation == Relation::STUDENTS) {

        for (const auto& student : relation_students) {
            auto tuple = std::make_unique<Tuple>();

            tuple->addField(std::make_unique<Field>(std::get<0>(student)));
            tuple->addField(std::make_unique<Field>(std::get<1>(student)));
            tuple->addField(std::make_unique<Field>(std::get<2>(student)));
            tuple->addField(std::make_unique<Field>("STUDENTS"));

            insert_operator.setTupleToInsert(std::move(tuple));
            bool status = insert_operator.next();
            
            assert(status == true);
        }
    } else if(relation == Relation::GRADES) {

        for (const auto& grade : relation_grades) {
            auto tuple = std::make_unique<Tuple>();

            tuple->addField(std::make_unique<Field>(std::get<0>(grade)));
            tuple->addField(std::make_unique<Field>(std::get<1>(grade)));
            tuple->addField(std::make_unique<Field>(std::get<2>(grade)));
            tuple->addField(std::make_unique<Field>("GRADES"));

            insert_operator.setTupleToInsert(std::move(tuple));
            bool status = insert_operator.next();
            
            assert(status == true);
        }
    }
    insert_operator.close();
}

int main(int argc, char* argv[]) {  
    bool execute_all = false;
    std::string selected_test = "-1";

    if(argc < 2) {
        execute_all = true;
    } else {
        selected_test = argv[1];
    }

    if(execute_all || selected_test == "1") {
        std::cout << "Executing Test 1 :: Field Comparision" << std::endl;
        Field field_i1(12345);
        Field field_i2(67890);
        Field field_i3(12345);

        Field field_s1("this is a string");
        Field field_s2("yet another stri");
        Field field_s3("this is a string");

        ASSERT_WITH_MESSAGE(field_i1 == field_i3, "Equal Field comparison failed");
        ASSERT_WITH_MESSAGE(field_i1 < field_i2, "Less-than Field comparison failed");
        ASSERT_WITH_MESSAGE(field_i1 != field_i2, "Not-equal Field comparison failed");

        ASSERT_WITH_MESSAGE(field_s1 == field_s3, "Equal Field comparison failed");
        ASSERT_WITH_MESSAGE(field_s1 < field_s2, "Less-than Field comparison failed");
        ASSERT_WITH_MESSAGE(field_s1 != field_s2, "Not-equal Field comparison failed");

        std::cout << "\033[1m\033[32mPassed: Test 1\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "2") {
        std::cout << "Executing Test 2 :: Print" << std::endl;
        BufferManager buffer_manager;

        TestTupleSource source(Relation::STUDENTS);
        std::stringstream output_stream;
        PrintOperator print_operator(source, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source.opened, "Source open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source.closed, "Source close failed");
        ASSERT_WITH_MESSAGE(output_stream.str() == sutdentsRelationToString(), "PrintOperator output does not match. Failed");

        std::cout << "\033[1m\033[32mPassed: Test 2\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "3") {
        std::cout << "Executing Test 3 :: Projection" << std::endl;
        BufferManager buffer_manager;
        std::vector<size_t> attributeIndices = {1,2};

        TestTupleSource source(Relation::STUDENTS);
        std::stringstream output_stream;

        ProjectOperator projection(source, attributeIndices);
        PrintOperator print_operator(projection, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source.opened, "Source open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source.closed, "Source close failed");
        auto expected_output = 
            ("Xenokrates, 24\n"
            "Fichte, 26\n"
            "Feuerbach, 29\n"
            "Schopenhauer, 46\n"
            "Platon, 50\n"
            "Aristoteles, 50\n"s);

        ASSERT_WITH_MESSAGE(output_stream.str() == expected_output, "ProjectOperator output does not match. Failed");
        
        std::cout << "\033[1m\033[32mPassed: Test 3\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "4") {
        std::cout << "Executing Test 4 :: Sort" << std::endl;
        BufferManager buffer_manager;
        std::vector<Sort::Criterion> criteria = { {1, true}, {2, false} };

        TestTupleSource source(Relation::GRADES);
        std::stringstream output_stream;

        Sort sort_operator(source, criteria);
        PrintOperator print_operator(sort_operator, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source.opened, "Source open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source.closed, "Source close failed");
        auto expected_output = 
            ("29555, 5041, 1\n"
            "24002, 5041, 2\n"
            "24002, 5001, 1\n"
            "24123, 5001, 2\n"
            "26120, 5001, 3\n"
            "25198, 4630, 1\n"
            "29555, 4630, 2\n"s);

        ASSERT_WITH_MESSAGE(output_stream.str() == expected_output, "SortOperator output does not match. Failed");

        std::cout << "\033[1m\033[32mPassed: Test 4\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "5") {
        std::cout << "Executing Test 5 :: HashJoin" << std::endl;
        
        TestTupleSource source_students(Relation::STUDENTS);
        TestTupleSource source_grades(Relation::GRADES);
        BufferManager buffer_manager;

        HashJoin join_operator(source_students, source_grades, 0, 0);
        std::stringstream output_stream;
        PrintOperator print_operator(join_operator, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source_students.opened, "Source students open failed");
        ASSERT_WITH_MESSAGE(source_grades.opened, "Source grades open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source_students.closed, "Source students close failed");
        ASSERT_WITH_MESSAGE(source_grades.closed, "Source grades close failed");

        auto expected_output = 
            ("24002, Xenokrates, 24, 24002, 5001, 1\n"
             "24002, Xenokrates, 24, 24002, 5041, 2\n"
             "24123, Platon, 50, 24123, 5001, 2\n"
             "25198, Aristoteles, 50, 25198, 4630, 1\n"
             "26120, Fichte, 26, 26120, 5001, 3\n"
             "29555, Feuerbach, 29, 29555, 4630, 2\n"
             "29555, Feuerbach, 29, 29555, 5041, 1\n"s);

        ASSERT_WITH_MESSAGE(sort_output(output_stream.str()) == expected_output, "HashJoin output does not match. Failed");

        std::cout << "\033[1m\033[32mPassed: Test 5\033[0m" << std::endl;
    }

        if(execute_all || selected_test == "6") {
        std::cout<<"Executing Test 6 :: HashAggregation - MinMax"<<std::endl;
        
        BufferManager buffer_manager;

        TestTupleSource source_students(Relation::STUDENTS);
        HashAggregationOperator hash_aggregation_operator(
            source_students,
            {},
            {
                {AggrFuncType::MIN, 1},
                {AggrFuncType::MAX, 1}
            });
        std::stringstream output_stream;
        PrintOperator print_operator(hash_aggregation_operator, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source_students.opened, "Source students open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source_students.closed, "Source students close failed");

        auto expected_output = ("Aristoteles, Xenokrates\n");

        ASSERT_WITH_MESSAGE(output_stream.str() == expected_output, "HashAggregation output does not match. Failed");

        std::cout<<"\033[1m\033[32mPassed: Test 6\033[0m"<<std::endl;
    }

    if(execute_all || selected_test == "7") {
        std::cout<<"Executing Test 7 :: HashAggregation - SumCount"<<std::endl;
        
        BufferManager buffer_manager;

        TestTupleSource source_grades(Relation::GRADES);
        HashAggregationOperator hash_aggregation_operator(
            source_grades,
            {0},
            {
                {AggrFuncType::SUM, 2},
                {AggrFuncType::COUNT, 0}
            });
        std::stringstream output_stream;
        PrintOperator print_operator(hash_aggregation_operator, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source_grades.opened, "Source students open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source_grades.closed, "Source students close failed");

        auto expected_output = 
            ("24002, 3, 2\n"
             "24123, 2, 1\n"
             "25198, 1, 1\n"
             "26120, 3, 1\n"
             "29555, 3, 2\n"s);

        ASSERT_WITH_MESSAGE(sort_output(output_stream.str()) == expected_output, "HashAggregation output does not match. Failed");

        std::cout<<"\033[1m\033[32mPassed: Test 7\033[0m"<<std::endl;
    }

    if(execute_all || selected_test == "8"){
        std::cout << "Executing Test 8 :: Union" << std::endl;
        BufferManager buffer_manager;

        TestTupleSource source_students(Relation::STUDENTS);
        TestTupleSource source_grades(Relation::GRADES);

        ProjectOperator project_students(source_students, {0});
        ProjectOperator project_grades(source_grades, {0});

        UnionOperator union_operator(project_students, project_grades);
        std::stringstream output_stream;
        PrintOperator print_operator(union_operator, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source_students.opened, "Source students open failed");
        ASSERT_WITH_MESSAGE(source_grades.opened, "Source grades open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source_students.closed, "Source students close failed");
        ASSERT_WITH_MESSAGE(source_grades.closed, "Source grades close failed");

        auto expected_output = 
            ("24002\n"
             "24123\n"
             "25198\n"
             "26120\n"
             "28000\n"
             "29555\n");

        ASSERT_WITH_MESSAGE(sort_output(output_stream.str()) == expected_output, "UnionOperator output does not match. Failed");

        std::cout << "\033[1m\033[32mPassed: Test 8\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "9"){
        std::cout << "Executing Test 9 :: UnionAll" << std::endl;
        BufferManager buffer_manager;

        TestTupleSource source_students(Relation::STUDENTS);
        TestTupleSource source_grades(Relation::GRADES);

        ProjectOperator project_students(source_students, {0});
        ProjectOperator project_grades(source_grades, {0});

        UnionAll union_operator(project_students, project_grades);
        std::stringstream output_stream;
        PrintOperator print_operator(union_operator, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source_students.opened, "Source students open failed");
        ASSERT_WITH_MESSAGE(source_grades.opened, "Source grades open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source_students.closed, "Source students close failed");
        ASSERT_WITH_MESSAGE(source_grades.closed, "Source grades close failed");

        auto expected_output = 
            ("24002\n"
             "24002\n"
             "24002\n"
             "24123\n"
             "24123\n"
             "25198\n"
             "25198\n"
             "26120\n"
             "26120\n"
             "28000\n"
             "29555\n"
             "29555\n"
             "29555\n");

        ASSERT_WITH_MESSAGE(sort_output(output_stream.str()) == expected_output, "UnionAll output does not match. Failed");

        std::cout << "\033[1m\033[32mPassed: Test 9\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "10"){
        std::cout << "Executing Test 10 :: Intersect" << std::endl;
        BufferManager buffer_manager;

        TestTupleSource source_students(Relation::STUDENTS);
        TestTupleSource source_grades(Relation::GRADES);

        ProjectOperator project_students(source_students, {0});
        ProjectOperator project_grades(source_grades, {0});

        Intersect intersect_operator(project_students, project_grades);
        std::stringstream output_stream;
        PrintOperator print_operator(intersect_operator, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source_students.opened, "Source students open failed");
        ASSERT_WITH_MESSAGE(source_grades.opened, "Source grades open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source_students.closed, "Source students close failed");
        ASSERT_WITH_MESSAGE(source_grades.closed, "Source grades close failed");

        auto expected_output = 
            ("24002\n"
             "24123\n"
             "25198\n"
             "26120\n"
             "29555\n");

        ASSERT_WITH_MESSAGE(sort_output(output_stream.str()) == expected_output, "Intersect output does not match. Failed");

        std::cout << "\033[1m\033[32mPassed: Test 10\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "11"){
        std::cout << "Executing Test 11 :: IntersectAll" << std::endl;
        BufferManager buffer_manager;

        TestTupleSource source_grades(Relation::GRADES);
        TestTupleSource source_feedback(Relation::FEEDBACK);

        ProjectOperator project_grades(source_grades, {1});
        ProjectOperator project_feedback(source_feedback, {0});

        IntersectAll intersect_operator(project_grades, project_feedback);
        std::stringstream output_stream;
        PrintOperator print_operator(intersect_operator, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source_feedback.opened, "Source feedback open failed");
        ASSERT_WITH_MESSAGE(source_grades.opened, "Source grades open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source_feedback.closed, "Source feedback close failed");
        ASSERT_WITH_MESSAGE(source_grades.closed, "Source grades close failed");

        auto expected_output = 
            ("4630\n"
             "4630\n"
             "5001\n"
             "5041\n"
             "5041\n");

        ASSERT_WITH_MESSAGE(sort_output(output_stream.str()) == expected_output, "IntersectAll output does not match. Failed");

        std::cout << "\033[1m\033[32mPassed: Test 11\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "12"){
        std::cout << "Executing Test 12 :: Except" << std::endl;
        BufferManager buffer_manager;

        TestTupleSource source_students(Relation::STUDENTS);
        TestTupleSource source_grades(Relation::GRADES);

        ProjectOperator project_students(source_students, {0});
        ProjectOperator project_grades(source_grades, {0});

        Except except_operator(project_students, project_grades);
        std::stringstream output_stream;
        PrintOperator print_operator(except_operator, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source_students.opened, "Source students open failed");
        ASSERT_WITH_MESSAGE(source_grades.opened, "Source grades open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source_students.closed, "Source students close failed");
        ASSERT_WITH_MESSAGE(source_grades.closed, "Source grades close failed");

        auto expected_output = 
            ("28000\n");

        ASSERT_WITH_MESSAGE(sort_output(output_stream.str()) == expected_output, "Except output does not match. Failed");

        std::cout << "\033[1m\033[32mPassed: Test 12\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "13"){
        std::cout << "Executing Test 13 :: ExceptAll" << std::endl;
        BufferManager buffer_manager;

        TestTupleSource source_students(Relation::STUDENTS);
        TestTupleSource source_grades(Relation::GRADES);

        ProjectOperator project_students(source_students, {0});
        ProjectOperator project_grades(source_grades, {0});

        ExceptAll except_operator(project_grades, project_students);
        std::stringstream output_stream;
        PrintOperator print_operator(except_operator, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source_students.opened, "Source students open failed");
        ASSERT_WITH_MESSAGE(source_grades.opened, "Source grades open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source_students.closed, "Source students close failed");
        ASSERT_WITH_MESSAGE(source_grades.closed, "Source grades close failed");

        auto expected_output = 
            ("24002\n"
             "29555\n");

        ASSERT_WITH_MESSAGE(sort_output(output_stream.str()) == expected_output, "ExceptAll output does not match. Failed");

        std::cout << "\033[1m\033[32mPassed: Test 13\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "14") {
        std::cout<<"Executing Test 14 :: JoinExecutor"<<std::endl;
        BufferManager buffer_manager;
        std::string query = "SELECT {*} FROM {STUDENTS} JOIN {GRADES} ON {1} = {1}";

        poplulate_db(Relation::STUDENTS, buffer_manager);
        poplulate_db(Relation::GRADES, buffer_manager);

        auto components = parseQuery(query);
        auto result = executeQuery(components, buffer_manager);

        //convert result to string
        std::stringstream result_stream;
        for(auto& tuple : result) {
            for (const auto& field : tuple) {
                result_stream<<field->asString();
                if(field != tuple.back())
                    result_stream << ", ";
            }
            result_stream << "\n";
        }

        auto expected_output = 
            ("24002, Xenokrates, 24, 24002, 5001, 1\n"
             "24002, Xenokrates, 24, 24002, 5041, 2\n"
             "24123, Platon, 50, 24123, 5001, 2\n"
             "25198, Aristoteles, 50, 25198, 4630, 1\n"
             "26120, Fichte, 26, 26120, 5001, 3\n"
             "29555, Feuerbach, 29, 29555, 4630, 2\n"
             "29555, Feuerbach, 29, 29555, 5041, 1\n"s);

        ASSERT_WITH_MESSAGE(sort_output(result_stream.str()) == expected_output, "JoinExecutor output does not match. Failed");

        std::cout<<"\033[1m\033[32mPassed: Test 14\033[0m"<<std::endl;
    }
}