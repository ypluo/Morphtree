enum {
    NUM_SOCKET = 2,
    NUM_PHYSICAL_CPU_PER_SOCKET = 8,
    SMT_LEVEL = 2,
};

const int OS_CPU_ID[NUM_SOCKET][NUM_PHYSICAL_CPU_PER_SOCKET][SMT_LEVEL] = {
    { /* socket id: 0 */
        { /* physical cpu id: 0 */
          0, 16,     },
        { /* physical cpu id: 1 */
          1, 17,     },
        { /* physical cpu id: 2 */
          2, 18,     },
        { /* physical cpu id: 3 */
          3, 19,     },
        { /* physical cpu id: 4 */
          4, 20,     },
        { /* physical cpu id: 5 */
          5, 21,     },
        { /* physical cpu id: 6 */
          6, 22,     },
        { /* physical cpu id: 7 */
          7, 23,     },
    },
    { /* socket id: 1 */
        { /* physical cpu id: 0 */
          8, 24,     },
        { /* physical cpu id: 1 */
          9, 25,     },
        { /* physical cpu id: 2 */
          10, 26,     },
        { /* physical cpu id: 3 */
          11, 27,     },
        { /* physical cpu id: 4 */
          12, 28,     },
        { /* physical cpu id: 5 */
          13, 29,     },
        { /* physical cpu id: 6 */
          14, 30,     },
        { /* physical cpu id: 7 */
          15, 31,     },
    },
};
