package com.praktikum.database.testing.dao;

import com.github.javafaker.Faker;
import com.praktikum.database.testing.BaseDatabaseTest;
import com.praktikum.database.testing.model.Book;
import com.praktikum.database.testing.model.Borrowing;
import com.praktikum.database.testing.model.User;
import com.praktikum.database.testing.utils.IndonesianFakerHelper;
import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive Data Integrity Test Suite
 * Menguji semua constraints, foreign keys, triggers, dan business rules
 * Fokus pada database-level validations dan relationships
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Data Integrity Test Suite")
public class DataIntegrityTest extends BaseDatabaseTest {

    private static UserDAO userDAO;
    private static BookDAO bookDAO;
    private static BorrowingDAO borrowingDAO;
    private static Faker faker;

    // Track all generated IDs for cleanup
    private static List<Integer> testUserIds;
    private static List<Integer> testBookIds;
    private static List<Integer> testBorrowingIds;

    @BeforeAll
    static void setUpAll() {
        logger.info("Starting Data Integrity Tests");

        userDAO = new UserDAO();
        bookDAO = new BookDAO();
        borrowingDAO = new BorrowingDAO();
        faker = IndonesianFakerHelper.getFaker();

        testUserIds = new ArrayList<>();
        testBookIds = new ArrayList<>();
        testBorrowingIds = new ArrayList<>();
    }

    @AfterAll
    static void tearDownAll() {
        logger.info("Data Integrity Tests Completed");
        cleanupTestData();
    }

    /**
     * Membersihkan semua data uji yang dibuat selama testing.
     */
    private static void cleanupTestData() {
        logger.info("Cleaning up all test data...");

        for (Integer borrowingId : testBorrowingIds) {
            try { borrowingDAO.delete(borrowingId); }
            catch (SQLException ignored) {}
        }

        for (Integer bookId : testBookIds) {
            try { bookDAO.delete(bookId); }
            catch (SQLException ignored) {}
        }

        for (Integer userId : testUserIds) {
            try { userDAO.delete(userId); }
            catch (SQLException ignored) {}
        }
    }

    // -------------------------------------------------------------------------
    // FOREIGN KEY CONSTRAINT TESTS
    // -------------------------------------------------------------------------

    @Test
    @Order(1)
    @DisplayName("TC301: Foreign Key - Valid borrowing creation dengan valid references")
    void testForeignKey_ValidBorrowingWithValidReferences_ShouldSuccess() throws SQLException {
        User user = userDAO.create(createTestUser());
        Book book = bookDAO.create(createTestBook());

        testUserIds.add(user.getUserId());
        testBookIds.add(book.getBookId());

        Borrowing borrowing = Borrowing.builder()
                .userId(user.getUserId())
                .bookId(book.getBookId())
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .status("borrowed")
                .build();

        Borrowing created = borrowingDAO.create(borrowing);
        testBorrowingIds.add(created.getBorrowingId());

        assertThat(created).isNotNull();
        assertThat(created.getUserId()).isEqualTo(user.getUserId());
        assertThat(created.getBookId()).isEqualTo(book.getBookId());
    }

    @Test
    @Order(2)
    @DisplayName("TC302: Foreign Key - Invalid user_id should violate constraint")
    void testForeignKey_InvalidUserId_ShouldFail() {
        Borrowing borrowing = Borrowing.builder()
                .userId(999999)
                .bookId(1)
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .build();

        assertThatThrownBy(() -> borrowingDAO.create(borrowing))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("foreign")
                .hasMessageContaining("user_id");
    }

    @Test
    @Order(3)
    @DisplayName("TC303: Foreign Key - Invalid book_id should violate constraint")
    void testForeignKey_InvalidBookId_ShouldFail() {
        Borrowing borrowing = Borrowing.builder()
                .userId(1)
                .bookId(999999)
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .build();

        assertThatThrownBy(() -> borrowingDAO.create(borrowing))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("foreign")
                .hasMessageContaining("book_id");
    }

    @Test
    @Order(4)
    @DisplayName("TC304: ON DELETE CASCADE - User deletion should cascade to Borrowings")
    void testOnDeleteCascade_UserDeletionCascadesToBorrowings() throws SQLException {
        User user = userDAO.create(createTestUser());
        Book book = bookDAO.create(createTestBook());

        testBookIds.add(book.getBookId());

        Borrowing created = borrowingDAO.create(Borrowing.builder()
                .userId(user.getUserId())
                .bookId(book.getBookId())
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .build());

        boolean deleted = userDAO.delete(user.getUserId());
        assertThat(deleted).isTrue();

        Optional<Borrowing> check = borrowingDAO.findById(created.getBorrowingId());
        assertThat(check).isEmpty();
    }

    @Test
    @Order(5)
    @DisplayName("TC305: ON DELETE RESTRICT - Book deletion with active borrowing should fail")
    void testOnDeleteRestrict_BookDeletionWithActiveBorrowing_ShouldFail() throws SQLException {
        User user = userDAO.create(createTestUser());
        Book book = bookDAO.create(createTestBook());

        testUserIds.add(user.getUserId());
        testBookIds.add(book.getBookId());

        Borrowing borrowing = borrowingDAO.create(Borrowing.builder()
                .userId(user.getUserId())
                .bookId(book.getBookId())
                .status("borrowed")
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .build());

        testBorrowingIds.add(borrowing.getBorrowingId());

        assertThatThrownBy(() -> bookDAO.delete(book.getBookId()))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("foreign");
    }

    // =========================================================================
    // CHECK CONSTRAINT TESTS
    // =========================================================================

    @Test
    @Order(6)
    @DisplayName("TC306: CHECK Constraint - due_date must be after borrow_date")
    void testCheckConstraint_DueDateAfterBorrowDate_ShouldFail() throws SQLException {
        User user = userDAO.create(createTestUser());
        Book book = bookDAO.create(createTestBook());

        testUserIds.add(user.getUserId());
        testBookIds.add(book.getBookId());

        Timestamp past = Timestamp.valueOf(LocalDateTime.now().minusDays(1));

        assertThatThrownBy(() -> borrowingDAO.create(
                Borrowing.builder()
                        .userId(user.getUserId())
                        .bookId(book.getBookId())
                        .dueDate(past)
                        .build()
        )).isInstanceOf(SQLException.class)
                .hasMessageContaining("check");
    }

    @Test
    @Order(7)
    @DisplayName("TC307: CHECK Constraint - available_copies <= total_copies")
    void testCheckConstraint_AvailableCopiesLessThanOrEqualTotal() {
        Book invalid = createTestBook();
        invalid.setTotalCopies(5);
        invalid.setAvailableCopies(10);

        assertThatThrownBy(() -> bookDAO.create(invalid))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("available_copies");
    }

    @Test
    @Order(8)
    @DisplayName("TC308: CHECK Constraint - User role must be valid enum value")
    void testCheckConstraint_ValidUserRole() {
        User invalid = createTestUser();
        invalid.setRole("superadmin");

        assertThatThrownBy(() -> userDAO.create(invalid))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("check");
    }

    @Test
    @Order(9)
    @DisplayName("TC309: CHECK Constraint - Book status must be valid enum value")
    void testCheckConstraint_ValidBookStatus() {
        Book book = bookDAO.create(createTestBook());
        testBookIds.add(book.getBookId());

        assertThatThrownBy(() ->
                executeSQL("UPDATE books SET status='invalid_status' WHERE book_id=" + book.getBookId())
        ).isInstanceOf(SQLException.class)
                .hasMessageContaining("check");
    }

    @Test
    @Order(10)
    @DisplayName("TC310: CHECK Constraint - Publication year within valid range")
    void testCheckConstraint_ValidPublicationYear() {
        Book invalid = createTestBook();
        invalid.setPublicationYear(999);

        assertThatThrownBy(() -> bookDAO.create(invalid))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("check");
    }

    // =========================================================================
    // UNIQUE CONSTRAINT TESTS
    // =========================================================================

    @Test
    @Order(11)
    @DisplayName("TC311: UNIQUE Constraint - Duplicate username should fail")
    void testUniqueConstraint_DuplicateUsername() throws SQLException {
        User first = userDAO.create(createTestUser());
        testUserIds.add(first.getUserId());

        User dup = createTestUser();
        dup.setUsername(first.getUsername());

        assertThatThrownBy(() -> userDAO.create(dup))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("unique")
                .hasMessageContaining("username");
    }

    @Test
    @Order(12)
    @DisplayName("TC312: UNIQUE Constraint - Duplicate email should fail")
    void testUniqueConstraint_DuplicateEmail() throws SQLException {
        User first = userDAO.create(createTestUser());
        testUserIds.add(first.getUserId());

        User dup = createTestUser();
        dup.setEmail(first.getEmail());

        assertThatThrownBy(() -> userDAO.create(dup))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("unique")
                .hasMessageContaining("email");
    }

    @Test
    @Order(13)
    @DisplayName("TC313: UNIQUE Constraint - Duplicate ISBN should fail")
    void testUniqueConstraint_DuplicateIsbn() throws SQLException {
        Book first = bookDAO.create(createTestBook());
        testBookIds.add(first.getBookId());

        Book dup = createTestBook();
        dup.setIsbn(first.getIsbn());

        assertThatThrownBy(() -> bookDAO.create(dup))
                .isInstanceOf(SQLException.class)
                .hasMessageContainingAnyOf("duplicate", "unique")
                .hasMessageContaining("isbn");
    }

    // =========================================================================
    // NOT NULL CONSTRAINT TESTS
    // =========================================================================

    @Test
    @Order(14)
    @DisplayName("TC314: NOT NULL Constraint - Username cannot be null")
    void testNotNullConstraint_UsernameCannotBeNull() {
        User invalid = createTestUser();
        invalid.setUsername(null);

        assertThatThrownBy(() -> userDAO.create(invalid))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("null");
    }

    @Test
    @Order(15)
    @DisplayName("TC315: NOT NULL Constraint - Email cannot be null")
    void testNotNullConstraint_EmailCannotBeNull() {
        User invalid = createTestUser();
        invalid.setEmail(null);

        assertThatThrownBy(() -> userDAO.create(invalid))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("null");
    }

    @Test
    @Order(16)
    @DisplayName("TC316: NOT NULL Constraint - Book title cannot be null")
    void testNotNullConstraint_BookTitleCannotBeNull() {
        Book invalid = createTestBook();
        invalid.setTitle(null);

        assertThatThrownBy(() -> bookDAO.create(invalid))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("null");
    }

    @Test
    @Order(17)
    @DisplayName("TC317: NOT NULL Constraint - Book ISBN cannot be null")
    void testNotNullConstraint_BookIsbnCannotBeNull() {
        Book invalid = createTestBook();
        invalid.setIsbn(null);

        assertThatThrownBy(() -> bookDAO.create(invalid))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("null");
    }

    // =========================================================================
    // TRIGGER TESTS
    // =========================================================================

    @Test
    @Order(18)
    @DisplayName("TC318: Trigger - updated_at auto-update on user update")
    void testTrigger_UpdatedAtAutoUpdateOnUserUpdate() throws SQLException, InterruptedException {
        User user = userDAO.create(createTestUser());
        testUserIds.add(user.getUserId());

        Timestamp before = user.getUpdatedAt();

        pause(1500); // menunggu biar timestamp berbeda

        user.setFullName("Updated Name");
        userDAO.update(user);

        Optional<User> updated = userDAO.findById(user.getUserId());
        assertThat(updated).isPresent();
        assertThat(updated.get().getUpdatedAt()).isAfter(before);
    }

    @Test
    @Order(19)
    @DisplayName("TC319: Trigger - updated_at auto-update on book update")
    void testTrigger_UpdatedAtAutoUpdateOnBookUpdate() throws SQLException, InterruptedException {
        Book book = bookDAO.create(createTestBook());
        testBookIds.add(book.getBookId());

        Timestamp before = book.getUpdatedAt();

        pause(1500);

        bookDAO.updateAvailableCopies(book.getBookId(), 3);
        Optional<Book> updated = bookDAO.findById(book.getBookId());

        assertThat(updated).isPresent();
        assertThat(updated.get().getUpdatedAt()).isAfter(before);
    }

    // =========================================================================
    // COMPLEX CONSTRAINT TESTS
    // =========================================================================

    @Test
    @Order(20)
    @DisplayName("TC320: Complex Constraint - Multiple borrowings of same book by different users")
    void testComplexConstraint_MultipleBorrowingsSameBookDifferentUsers() throws SQLException {
        User user1 = userDAO.create(createTestUser());
        User user2 = userDAO.create(createTestUser());
        Book book = bookDAO.create(createTestBook());

        testUserIds.add(user1.getUserId());
        testUserIds.add(user2.getUserId());
        testBookIds.add(book.getBookId());

        Borrowing b1 = borrowingDAO.create(Borrowing.builder()
                .userId(user1.getUserId())
                .bookId(book.getBookId())
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .build());

        Borrowing b2 = borrowingDAO.create(Borrowing.builder()
                .userId(user2.getUserId())
                .bookId(book.getBookId())
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .build());

        testBorrowingIds.add(b1.getBorrowingId());
        testBorrowingIds.add(b2.getBorrowingId());

        Optional<Book> updated = bookDAO.findById(book.getBookId());
        assertThat(updated).isPresent();
        assertThat(updated.get().getAvailableCopies()).isEqualTo(3);
    }

    // =========================================================================
    // HELPER METHODS
    // =========================================================================

    private User createTestUser() {
        return User.builder()
                .username("user_" + System.currentTimeMillis() + "_" + faker.number().randomNumber())
                .email(IndonesianFakerHelper.generateIndonesianEmail())
                .fullName(IndonesianFakerHelper.generateIndonesianName())
                .phone(IndonesianFakerHelper.generateIndonesianPhone())
                .role("member")
                .status("active")
                .build();
    }

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
                .availableCopies(3)
                .price(new BigDecimal("75000.00"))
                .location("Rak Integrity-Test")
                .status("available")
                .build();
    }
}
