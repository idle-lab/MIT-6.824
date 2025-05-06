#include <atomic>
#include <latch>

int main() {
    std::latch st(1);
    st.try_wait();
    st.count_down();
    st.arrive_and_wait();

    std::atomic_flag af = ATOMIC_FLAG_INIT;
    af.test();

    std::atomic<int> a;
    a.is_lock_free();
    return 0;
}