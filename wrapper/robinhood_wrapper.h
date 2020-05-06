#ifndef ROBINHOOD_WRAPPER
#define ROBINHOOD_WRAPPER

#include "hashtable/hashtable.hpp"
#include "wrapper/stupid_iterator.h"

class RobinhoodWrapper
{
private:
    using HashType = Hashtable;

    HashType hash;
    size_t capacity;

public:
    using key_type           = size_t;
    using mapped_type        = size_t;
    using value_type         = typename std::pair<const key_type, mapped_type>;
    using iterator           = StupidIterator<key_type, mapped_type>;
    using insert_return_type = std::pair<iterator, bool>;

    RobinhoodWrapper() = delete;
    RobinhoodWrapper(size_t capacity_) : hash(capacity_), capacity(capacity_) {}
    RobinhoodWrapper(const RobinhoodWrapper&) = delete;
    RobinhoodWrapper& operator=(const RobinhoodWrapper&) = delete;

    RobinhoodWrapper(RobinhoodWrapper&& rhs) : hash(rhs.capacity), capacity(rhs.capacity) {}

    RobinhoodWrapper& operator=(RobinhoodWrapper&& rhs)
    {
        capacity = rhs.capacity;
        (& hash)->~HashType();
        new (& hash) HashType(rhs.capacity);
        return *this;
    }

    using Handle = RobinhoodWrapper&;
    Handle get_handle() { return *this; }
    
    inline iterator find(const key_type& k)
    {
      bool found;
      valtype v = hash.Get(k, found);
      if(found) {
        return iterator(k, v);
      } else {
        return end();
      }
    }

    inline insert_return_type insert(const key_type &k, const mapped_type &d) {
      bool inserted = hash.Insert(k, d);
      return {{inserted ? k : 0, d}, inserted};
    }

    template<class F, class ... Types>
    inline insert_return_type update(const key_type& k, F f, Types&& ... args)
    {
        //if (! F::junction_compatible::value) return insert_return_type(end(), false);

        // extract the update value
        auto update_value = mapped_type();
        f(update_value, std::forward<Types>(args)...);

        // update only
        bool updated = hash.Update(k, update_value);
        return {{updated ? k : 0, update_value}, updated};
    }

    template<class F, class ... Types>
    inline insert_return_type insert_or_update(const key_type& k, const mapped_type& d, __attribute__((unused)) F f, __attribute__((unused)) Types&& ... args)
    {
        //if (! F::junction_compatible::value) return insert_return_type(end(), false);

        // extract the update value
        auto update_value = mapped_type();
        f(update_value, std::forward<Types>(args)...);

        // insert or update
        bool inserted = hash.InsertOrUpdate(k, d, update_value);

        // construct the return value
        if(inserted) return {iterator(k, d), true}; // inserted
        else return {iterator(k, update_value), false}; // updated
    }

    template<class F, class ... Types>
    inline insert_return_type update_unsafe(const key_type& k, F f, Types&& ... args)
    {
        return update(k,f,std::forward<Types>(args)...);
    }

    template<class F, class ... Types>
    inline insert_return_type insert_or_update_unsafe(const key_type& k, const mapped_type& d, F f, Types&& ... args)
    {
        return insert_or_update(k,d,f,std::forward<Types>(args)...);
    }

    inline size_t erase(const key_type& k)
    {
        return hash.Delete(k) ? 1ul : 0ul;
    }

    inline iterator end() { return iterator(); }
};
#endif // ROBINHOOD_WRAPPER
