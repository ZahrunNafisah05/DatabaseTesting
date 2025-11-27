package com.praktikum.database.testing.dao;

import com.github.javafaker.Faker;
import com.praktikum.database.testing.BaseDatabaseTest;
import com.praktikum.database.testing.model.Borrowing;
import com.praktikum.database.testing.model.User;
import com.praktikum.database.testing.model.Book;
import com.praktikum.database.testing.utils.IndonesianFakerHelper;
import org.junit.jupiter.api.*;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

// Import static assertions
import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive Data Integrity Test Suite
 * - Menguji semua constraints, foreign keys, triggers, dan business
 * rules
 * - Focus pada database-level validations dan relationships
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Data Integrity Test Suite")
public class DataIntegrityTest extends BaseDatabaseTest {

    // Test dependencies
    private static UserDAO userDAO;
    private static BookDAO bookDAO;
    private static BorrowingDAO borrowingDAO;
    private static Faker faker;

    // Test data trackers
    private static List<Integer> testUserIds;
    private static List<Integer> testBookIds;
    private static List<Integer> testBorrowingIds;

    @BeforeAll
    static void setUpAll() {
        logger.info("Starting Data Integrity Tests");
        // Initialize semua DAOs
        userDAO = new UserDAO();
        bookDAO = new BookDAO();
        borrowingDAO = new BorrowingDAO();
        faker = IndonesianFakerHelper.getFaker();

        // Initialize trackers
        testUserIds = new java.util.ArrayList<>();
        testBookIds = new java.util.ArrayList<>();
        testBorrowingIds = new java.util.ArrayList<>();
    }
    @AfterAll
    static void tearDownAll() {
        logger.info("Data Integrity Tests Completed");
        // Cleanup semua test data
        cleanupTestData();
    }

    /**
     * Cleanup semua test data yang dibuat selama testing
     */
    private static void cleanupTestData() {
        logger.info("Cleaning up all test data...");

        // Cleanup borrowings terlebih dahulu (karena foreign key constraints)
        for (Integer borrowingId : testBorrowingIds) {
            try {
                borrowingDAO.delete(borrowingId);
            } catch (SQLException e) {
                logger.warning("Gagal cleanup borrowing ID: " +
                        borrowingId);
            }
        }

        // Cleanup books
        for (Integer bookId : testBookIds) {
            try {
                bookDAO.delete(bookId);
            } catch (SQLException e) {
                logger.warning("Gagal cleanup book ID: " + bookId);
            }
        }

        // Cleanup users
        for (Integer userId : testUserIds) {
            try {
                userDAO.delete(userId);
            } catch (SQLException e) {
                logger.warning("Gagal cleanup user ID: " + userId);
            }
        }
    }

// FOREIGN KEY CONSTRAINT TESTS
// FOREIGN KEY CONSTRAINT TESTS
// -----------------------------------------------------

    @Test
    @Order(1)
    @DisplayName("TC301: Foreign Key - Valid borrowing creation dengan valid references")
    void testForeignKey_ValidBorrowingWithValidReferences_ShouldSuccess()
            throws SQLException {
        // ARRANGE - Create user dan book terlebih dahulu
        User user = createUserTestUser();
        Book book = createTestBook();

        user = userDAO.create(user);
        book = bookDAO.create(book);

        testUserIds.add(user.getUserId());
        testBookIds.add(book.getBookId());

        Borrowing borrowing = Borrowing.builder()
                .userId(user.getUserId())
                .bookId(book.getBookId())
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .status("borrowed")
                .build();

        // ACT - Create borrowing dengan valid foreign keys
        Borrowing created = borrowingDAO.create(borrowing);
        testBorrowingIds.add(created.getBorrowingId());

        // ASSERT - Borrowing harus berhasil dibuat
        assertThat(created).isNotNull();
        assertThat(created.getBorrowingId()).isNotNull();
        assertThat(created.getUserId()).isEqualTo(user.getUserId());
        assertThat(created.getBookId()).isEqualTo(book.getBookId());

        logger.info("TC301 PASSED: Valid foreign keys accepted - Borrowing ID: "
                + created.getBorrowingId());
    }

    @Test
    @Order(2)
    @DisplayName("TC302: Foreign Key - Invalid user_id should violate constraint")
    void testForeignKey_InvalidUserId_ShouldFail()
            throws SQLException {
        // ARRANGE - Borrowing dengan user_id yang tidak ada di database
        // Note: Kita tidak perlu membuat book karena akan gagal di user_id dulu
        Borrowing borrowing = Borrowing.builder()
                .userId(99999) // Invalid user_id - tidak ada di database
                .bookId(1) // Asumsi book_id 1 exists dari sample data
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .build();

        // ACT & ASSERT - Harus throw SQLException dengan foreign key violation
        assertThatThrownBy(() -> borrowingDAO.create(borrowing))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("foreign key") // Harus contain "foreign key"
                .hasMessageContaining("user_id"); // Dan "user_id"

        logger.info("TC302 PASSED: Foreign key constraint for user_id working correctly");
    }
    @Test
    @Order(3)
    @DisplayName("TC303: Foreign Key - Invalid book_id should violate constraint")
    void testForeignKey_InvalidBookId_ShouldFail()
            throws SQLException {
        // ARRANGE - Borrowing dengan book_id yang tidak ada di database
        // Note: Kita tidak perlu membuat user karena akan gagal di book_id dulu
        Borrowing borrowing = Borrowing.builder()
                .userId(1) // Asumsi user_id 1 exists dari sample data
                .bookId(99999) // Invalid book_id - tidak ada di database
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .build();

        // ACT & ASSERT
        assertThatThrownBy(() -> borrowingDAO.create(borrowing))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("foreign key")
                .hasMessageContaining("book_id");

        logger.info("TC303 PASSED: Foreign key constraint for book_id working correctly");
    }

    @Test
    @Order(4)
    @DisplayName("TC304: ON DELETE CASCADE - User deletion should cascade to borrowings")
    void testOnDeleteCascade_UserDeletionCascadesToBorrowings()
            throws SQLException {
        // ARRANGE - Create user, book, dan borrowing
        User user = createUserTestUser();
        Book book = createTestBook();

        user = userDAO.create(user);
        book = bookDAO.create(book);

        // cleanup
        testBookIds.add(book.getBookId()); // Track book untuk cleanup
        // Jangan track user karena akan di-delete

        Borrowing borrowing = Borrowing.builder()
                .userId(user.getUserId())
                .bookId(book.getBookId())
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .build();

        Borrowing created = borrowingDAO.create(borrowing);
        // Jangan track borrowing karena akan ter-delete cascade

        // ACT - Delete user (should CASCADE delete borrowing)
        boolean userDeleted = userDAO.delete(user.getUserId());

        // ASSERT - User harus ter-delete
        assertThat(userDeleted).isTrue();

        // VERIFY - Borrowing juga harus terhapus (CASCADE effect)
        Optional<Borrowing> deletedBorrowing = borrowingDAO.findById(created.getBorrowingId());
        assertThat(deletedBorrowing).isEmpty(); // Borrowing harus sudah tidak ada

        logger.info("TC304 PASSED: ON DELETE CASCADE working correctly for users -> borrowings");
    }
    @Test
    @Order(5)
    @DisplayName("TC305: ON DELETE RESTRICT - Book deletion with active borrowing should fail")
    void testOnDeleteRestrict_BookDeletionWithActiveBorrowing_ShouldFail()
            throws SQLException {
        // ARRANGE - Create user, book, dan active borrowing
        User user = createUserTestUser();
        Book book = createTestBook();

        user = userDAO.create(user);
        book = bookDAO.create(book);

        testUserIds.add(user.getUserId());
        testBookIds.add(book.getBookId()); // Track book (tapi tidak jadi di-delete)

        Borrowing borrowing = Borrowing.builder()
                .userId(user.getUserId())
                .bookId(book.getBookId())
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .status("borrowed")
                .build();

        Borrowing created = borrowingDAO.create(borrowing);
        testBorrowingIds.add(created.getBorrowingId());

        // Buat final copy untuk digunakan dalam lambda
        final Integer finalBookId = book.getBookId();

        // ACT & ASSERT - Try to delete book (should fail - ON DELETE RESTRICT)
        assertThatThrownBy(() -> bookDAO.delete(finalBookId))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("foreign key"); // Foreign key constraint violation

        logger.info("TC305 PASSED: ON DELETE RESTRICT working correctly for books with active borrowings");
    }

// CHECK CONSTRAINT TESTS
// CHECK CONSTRAINT TESTS
// -----------------------------------------------------

    @Test
    @Order(6)
    @DisplayName("TC306: CHECK Constraint - due_date must be after borrow_date")
    void testCheckConstraint_DueDateAfterBorrowDate()
            throws SQLException {
        // ARRANGE - due_date di masa lalu (sebelum borrow_date)
        User user = createUserTestUser();
        Book book = createTestBook();

        // Simpan user dan book terlebih dahulu
        user = userDAO.create(user);
        book = bookDAO.create(book);
        testUserIds.add(user.getUserId());
        testBookIds.add(book.getBookId());

        // Buat final copies untuk digunakan dalam lambda
        final Integer finalUserId = user.getUserId();
        final Integer finalBookId = book.getBookId();

        Timestamp pastDate = Timestamp.valueOf(LocalDateTime.now().minusDays(1)); // Invalid: due_date di masa lalu

        // ACT & ASSERT
        assertThatThrownBy(() -> Borrowing.builder()
                .userId(finalUserId)
                .bookId(finalBookId)
                .dueDate(pastDate) // Invalid: due_date sebelum borrow_date
                .build())
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("check"); // CHECK constraint violation

        logger.info("TC306 PASSED: CHECK constraint for due_date > borrow_date working correctly");
    }

    @Test
    @Order(7)
    @DisplayName("TC307: CHECK Constraint - available_copies <= total_copies")
    void testCheckConstraint_AvailableCopiesLessThanOrEqualtoTotal()
            throws SQLException {
        // ARRANGE - available_copies > total_copies
        Book invalidBook = createTestBook();
        invalidBook.setTotalCopies(5);
        invalidBook.setAvailableCopies(10); // BUG: 10 > 5 - violates constraint

        // ACT & ASSERT
        assertThatThrownBy(() -> bookDAO.create(invalidBook))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("check_available_copies"); // Specific constraint name

        logger.info("TC307 PASSED: CHECK constraint for check_available_copies working correctly");
    }
    @Test
    @Order(8)
    @DisplayName("TC308: CHECK Constraint - User role must be valid enum value")
    void testCheckConstraint_ValidUserRole()
            throws SQLException {
        // ARRANGE - Invalid user role
        User invalidUser = createUserTestUser();
        invalidUser.setRole("superadmin"); // Invalid role (bukan member, librarian, admin)

        // ACT & ASSERT
        assertThatThrownBy(() -> userDAO.create(invalidUser))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("check"); // CHECK constraint violation

        logger.info("TC308 PASSED: CHECK constraint for user role working correctly");
    }

    @Test
    @Order(9)
    @DisplayName("TC309: Check Constraint - Book status must be valid enum value")
    void testCheckConstraint_ValidBookStatus()
            throws SQLException {
        // ARRANGE - Create book terlebih dahulu
        Book book = createTestBook();
        book = bookDAO.create(book);
        testBookIds.add(book.getBookId());

        // Buat final copy untuk digunakan dalam lambda
        final Integer finalBookId = book.getBookId();

        // ACT & ASSERT - Try to update dengan invalid status
        // Note: Kita test via direct SQL karena DAO method tidak mengizinkan invalid status
        assertThatThrownBy(() -> executeSQL("UPDATE books SET status = 'invalid_status' WHERE book_id = " + finalBookId))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("check"); // CHECK constraint violation

        logger.info("TC309 PASSED: CHECK constraint for book status working correctly");
    }

    @Test
    @Order(10)
    @DisplayName("TC310: CHECK Constraint - Publication year within valid range")
    void testCheckConstraint_ValidPublicationYear()
            throws SQLException {
        // ARRANGE - Invalid publication year
        Book invalidBook = createTestBook();
        invalidBook.setPublicationYear(999); // Invalid: < 1000

        // ACT & ASSERT
        assertThatThrownBy(() -> bookDAO.create(invalidBook))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("check"); // CHECK constraint for publication year

        logger.info("TC310 PASSED: CHECK constraint for publication year working correctly");
    }
    // UNIQUE CONSTRAINT TESTS
// -----------------------------------------------------

    @Test
    @Order(11)
    @DisplayName("TC311: UNIQUE Constraint - Duplicate username should fail")
    void testUniqueConstraint_DuplicateUsername() throws SQLException {
        // ARRANGE - Create first user
        User firstUser = createUserTestUser();
        firstUser = userDAO.create(firstUser);
        testUserIds.add(firstUser.getUserId());

        // ARRANGE - Second user dengan username yang sama
        User duplicateUser = createUserTestUser(); // Diperbaiki: createTestUser() -> createUserTestUser()
        duplicateUser.setUsername(firstUser.getUsername()); // Same username

        // ACT & ASSERT
        assertThatThrownBy(() -> userDAO.create(duplicateUser))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("unique") // UNIQUE constraint violation
                .hasMessageContaining("username");

        logger.info("TC311 PASSED: UNIQUE constraint for username working correctly");
    }

    @Test
    @Order(12)
    @DisplayName("TC312: UNIQUE Constraint - Duplicate email should fail")
    void testUniqueConstraint_DuplicateEmail() throws SQLException {
        // ARRANGE - Create first user
        User firstUser = createUserTestUser();
        firstUser = userDAO.create(firstUser);
        testUserIds.add(firstUser.getUserId());

        // ARRANGE - Second user dengan email yang sama
        User duplicateUser = createUserTestUser(); // Diperbaiki: createTestUser() -> createUserTestUser()
        duplicateUser.setEmail(firstUser.getEmail()); // Same email

        // ACT & ASSERT
        assertThatThrownBy(() -> userDAO.create(duplicateUser))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("unique")
                .hasMessageContaining("email");

        logger.info("TC312 PASSED: UNIQUE constraint for email working correctly");
    }

    @Test
    @Order(13)
    @DisplayName("TC313: UNIQUE Constraint - Duplicate ISBN should fail")
    void testUniqueConstraint_DuplicateIsbn() throws SQLException {
        // ARRANGE - Create first book
        Book firstBook = createTestBook();
        firstBook = bookDAO.create(firstBook);
        testBookIds.add(firstBook.getBookId());

        // ARRANGE - Second book dengan ISBN yang sama
        Book duplicateBook = createTestBook();
        duplicateBook.setIsbn(firstBook.getIsbn()); // Same ISBN

        // ACT & ASSERT
        assertThatThrownBy(() -> bookDAO.create(duplicateBook))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("duplicate") // Bisa "duplicate" atau "unique"
                .hasMessageContaining("isbn");

        logger.info("TC313 PASSED: UNIQUE constraint for ISBN working correctly");
    }
    // NOT NULL CONSTRAINT TESTS
// -----------------------------------------------------

    @Test
    @Order(14)
    @DisplayName("TC314: NOT NULL Constraint - Username cannot be null")
    void testNotNullConstraint_UsernameCannotBeNull() {
        // ARRANGE - User dengan username null
        User nullUser = createUserTestUser();
        nullUser.setUsername(null); // Violates NOT NULL constraint

        // ACT & ASSERT
        assertThatThrownBy(() -> userDAO.create(nullUser))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("null"); // NULL constraint violation

        logger.info("TC314 PASSED: NOT NULL constraint for username working correctly");
    }

    @Test
    @Order(15)
    @DisplayName("TC315: NOT NULL Constraint - Email cannot be null")
    void testNotNullConstraint_EmailCannotBeNull() {
        // ARRANGE - User dengan email null
        User nullUser = createUserTestUser();
        nullUser.setEmail(null); // Violates NOT NULL constraint

        // ACT & ASSERT
        assertThatThrownBy(() -> userDAO.create(nullUser))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("null");

        logger.info("TC315 PASSED: NOT NULL constraint for email working correctly");
    }

    @Test
    @Order(16)
    @DisplayName("TC316: NOT NULL Constraint - Book title cannot be null")
    void testNotNullConstraint_BookTitleCannotBeNull() {
        // ARRANGE - Book dengan title null
        Book nullBook = createTestBook();
        nullBook.setTitle(null); // Violates NOT NULL constraint

        // ACT & ASSERT
        assertThatThrownBy(() -> bookDAO.create(nullBook))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("null");

        logger.info("TC316 PASSED: NOT NULL constraint for book title working correctly");
    }

    @Test
    @Order(17)
    @DisplayName("TC317: NOT NULL Constraint - Book ISBN cannot be null")
    void testNotNullConstraint_BookIsbnCannotBeNull() {
        // ARRANGE - Book dengan ISBN null
        Book nullBook = createTestBook();
        nullBook.setIsbn(null); // Violates NOT NULL constraint

        // ACT & ASSERT
        assertThatThrownBy(() -> bookDAO.create(nullBook))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("null");

        logger.info("TC317 PASSED: NOT NULL constraint for book ISBN working correctly");
    }

// TRIGGER TESTS
// TRIGGER TESTS
// -----------------------------------------------------

    @Test
    @Order(18)
    @DisplayName("TC318: Trigger - updated_at auto-update on user update")
    void testTrigger_UpdatedAtAutoUpdateOnUserUpdate()
            throws SQLException, InterruptedException {
        // ARRANGE - Create user dan get original updated_at
        User user = createUserTestUser();
        user = userDAO.create(user);
        testUserIds.add(user.getUserId());

        Timestamp originalUpdatedAt = user.getUpdatedAt();

        // Tunggu 2 detik untuk memastikan timestamp berbeda
        pause(2000);

        // ACT - Update user
        user.setFullName("Updated Name for Trigger Test");
        userDAO.update(user);

        // ACT - Verify updated_at changed
        // Lanjutan dari TC318: Trigger - updated_at auto-update on user update

        Optional<User> updatedUser =
                userDAO.findById(user.getUserId());
        assertThat(updatedUser)
                .isPresent()
                .get()
                .satisfies(u -> {
                    assertThat(u.getUpdatedAt())
                            .isAfter(originalUpdatedAt); // updated_at harus lebih baru
                });

        logger.info("TC318 PASSED: Trigger updated_at working correctly for users");
        logger.info("Before: " + originalUpdatedAt);
        logger.info("After: " +
                updatedUser.get().getUpdatedAt());
    }

    @Test
    @Order(19)
    @DisplayName("TC319: Trigger - updated_at auto-update on book update")
    void testTrigger_UpdatedAtAutoUpdateOnBookUpdate()
            throws SQLException, InterruptedException {
        // ARRANGE - Create book dan get original updated_at
        Book book = createTestBook();
        book = bookDAO.create(book);
        testBookIds.add(book.getBookId());

        Timestamp originalUpdatedAt = book.getUpdatedAt();

        // Tunggu 2 detik
        pause(2000);

        // ACT - Update book
        book.setTitle("Updated Title for Trigger Test");
        // Note: Kita perlu update di BookDAO, jadi kita gunakan available copies update
        bookDAO.updateAvailableCopies(book.getBookId(), 3);

        // ASSERT - Verify updated_at changed
        Optional<Book> updatedBook =
                bookDAO.findById(book.getBookId());
        assertThat(updatedBook)
                .isPresent()
                .get()
                .satisfies(b -> {
                    assertThat(b.getUpdatedAt())
                            .isAfter(originalUpdatedAt);
                });

        logger.info("TC319 PASSED: Trigger updated_at working correctly for books");
    }

// COMPLEX CONSTRAINT TESTS
// -----------------------------------------------------

    @Test
    @Order(20)
    @DisplayName("TC320: Complex Constraint - Multiple borrowings of same book by different users")
    void testComplexConstraint_MultipleBorrowingsSameBookDifferentUsers()
            throws SQLException {
        // ARRANGE - Create two users dan one book
        User user1 = createUserTestUser();
        User user2 = createUserTestUser();
        Book book = createTestBook();

        user1 = userDAO.create(user1);
        user2 = userDAO.create(user2);
        book = bookDAO.create(book);

        testUserIds.add(user1.getUserId());
        testUserIds.add(user2.getUserId());
        testBookIds.add(book.getBookId());

        // Buat final copies untuk digunakan dalam assertion
        final Integer finalUserId1 = user1.getUserId();
        final Integer finalUserId2 = user2.getUserId();
        final Integer finalBookId = book.getBookId();

        Borrowing borrowing1 = Borrowing.builder()
                .userId(finalUserId1)
                .bookId(finalBookId)
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .build();

        Borrowing borrowing2 = Borrowing.builder()
                .userId(finalUserId2)
                .bookId(finalBookId)
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .build();

        // ACT - Both users borrow the same book
        Borrowing created1 = borrowingDAO.create(borrowing1);
        Borrowing created2 = borrowingDAO.create(borrowing2);
        testBorrowingIds.add(created1.getBorrowingId());
        testBorrowingIds.add(created2.getBorrowingId());

        // ASSERT - Both borrowings should be created successfully
        assertThat(created1).isNotNull();
        assertThat(created2).isNotNull();

        // VERIFY - Available copies should decrease by 2
        Optional<Book> updatedBook = bookDAO.findById(finalBookId);
        assertThat(updatedBook)
                .isPresent()
                .get()
                .satisfies(b ->
                        assertThat(b.getAvailableCopies()).isEqualTo(3)); // 5 - 2 = 3

        logger.info("TC320 PASSED: Multiple borrowings of same book allowed for different users");
        logger.info("Available copies: 5 - " + (5 - updatedBook.get().getAvailableCopies()));
    }
    // HELPER METHODS
// -----------------------------------------------------

    /**
     * Helper method untuk membuat test user
     */
    private User createUserTestUser() { // Nama method yang benar
        return User.builder()
                .username("user_" + System.currentTimeMillis() + "-" + faker.number().randomNumber())
                .email(faker.internet().emailAddress()) // Menggunakan faker object
                .fullName(faker.name().fullName()) // Menggunakan faker object untuk menghindari error resolve method
                .phone(faker.phoneNumber().phoneNumber()) // Menggunakan faker object
                .role("member")
                .status("active")
                .build();
    }

    /**
     * Helper method untuk membuat test book
     */
    private Book createTestBook() {
        return Book.builder()
                .isbn("978id" + System.currentTimeMillis())
                .title("Buku Integrity Test - " + faker.book().title())
                .authorId(1)
                .publisherId(1)
                .categoryId(1)
                .publicationYear(2023)
                .pages(300)
                .language("Indonesia")
                .description("Buku untuk testing integrity - " + faker.lorem().sentence())
                .totalCopies(5)
                .availableCopies(5)
                .price(new java.math.BigDecimal("75000.00"))
                .location("Test Integrity-Test")
                .status("available")
                .build();
    }
}