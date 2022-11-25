#include "primer/p0_trie.h"

// This is a placeholder file for clang-tidy check.
//
// With this file, we can fire run_clang_tidy.py to check `p0_trie.h`,
// as it will filter out all header files and won't check header-only code.
//
// This file is not part of the submission. All of the modifications should
// be done in `src/include/primer/p0_trie.h`.

namespace bustub {

TrieNode::TrieNode(char key_char) : key_char_(key_char), is_end_(false) {

}

TrieNode::TrieNode(TrieNode &&other_trie_node) noexcept
    : key_char_(other_trie_node.GetKeyChar())
    , is_end_(other_trie_node.IsEndNode()) {
        children_ = std::move(other_trie_node.children_);
}

bool TrieNode::HasChild(char key_char) const { 
    auto it = children_.find(key_char);
    if(it != children_.end())
        return true;

    return false; 
}

bool TrieNode::HasChildren() const {
    if(children_.size() > 0)
        return true;

    return false; 
}

bool TrieNode::IsEndNode() const { 
    return is_end_; 
}

char TrieNode::GetKeyChar() const { 
    return key_char_; 
}

std::unique_ptr<TrieNode>* TrieNode::InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) { 
    auto it = children_.find(key_char);
    if(it == children_.end() && key_char == child->GetKeyChar()) {
        children_[key_char] = std::move(child);
        return &children_[key_char];
    }
    
    return nullptr; 
}

std::unique_ptr<TrieNode>* TrieNode::GetChildNode(char key_char) { 
    auto it = children_.find(key_char);
    if(it != children_.end())
        return &it->second;

    return nullptr; 
}

void TrieNode::RemoveChildNode(char key_char) {
    auto it = children_.find(key_char);
    if(it != children_.end())
        children_.erase(it);
}

void TrieNode::SetEndNode(bool is_end) {
    is_end_ = is_end;
}

Trie::Trie()
    : root_(std::make_unique<TrieNode>(TrieNode('\0'))) {

}

}