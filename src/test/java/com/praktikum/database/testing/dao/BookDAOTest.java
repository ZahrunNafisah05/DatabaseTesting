package com.praktikum.database.testing.dao;

import com.github.javafaker.Faker;
import com.praktikum.database.testing.BaseDatabaseTest;
import com.praktikum.database.testing.model.Book;
import org.junit.jupiter.api.*;

import java.sql.SQLException;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive test suite untuk BookDAO
 * Menguji semua operasi CRUD & business logic (available copies, search, constraints)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("BookDAO CRUD Operations Test Suite")
public class BookDAOTest extends BaseDatabaseTest {

    private static BookDAO bookDAO;
    private static Faker faker;

    private static Book testBook;
    private static List<Integer> createdBookIds;

    // ------------------------------------------------------------------------------------------------ //
    // SETUP & TEAR DOWN
    // ------------------------------------------------------------------------------------------------ //

    @BeforeAll
    static void setUpAll() {
        logger.info("Starting BookDAO CRUD Tests");

        bookDAO = new BookDAO();
        faker = new Faker();
        createdBookIds = new ArrayList<>();
    }

    @AfterAll
    static void tearDownAll() throws SQLException {
        logger.info("BookDAO CRUD Tests Completed");

        for (Integer bookId : createdBookIds) {
            try {
                bookDAO.delete(bookId);
                logger.fine("Cleaned up test book ID: " + bookId);
            } catch (SQLException e) {
                logger.warning("Gagal cleanup book ID: " + bookId + " - " + e.getMessage());
            }
        }
    }

    // ------------------------------------------------------------------------------------------------ //
    // POSITIVE TEST CASES
    // ------------------------------------------------------------------------------------------------ //

    @Test
    @Order(1)
    @DisplayName("TC101: Create book dengan data valid - Should Success")
    void testCreateBook_WithValidData_ShouldSuccess() throws SQLException {

        testBook = createTestBook();

        Book createdBook = bookDAO.create(testBook);
        createdBookIds.add(createdBook.getBookId());

        assertThat(createdBook)
                .isNotNull()
                .satisfies(book -> {
                    assertThat(book.getBookId()).isPositive();
                    assertThat(book.getIsbn()).isEqualTo(testBook.getIsbn());
                    assertThat(book.getAvailableCopies()).isEqualTo(5);
                });

        logger.info("TC101 PASSED");
    }

    @Test
    @Order(2)
    @DisplayName("TC102: Find book by existing ID - Should Return Book")
    void testFindBookById_ShouldReturnBook() throws SQLException {

        Optional<Book> foundBook = bookDAO.findById(testBook.getBookId());

        assertThat(foundBook)
                .isPresent()
                .get()
                .satisfies(book -> assertThat(book.getBookId()).isEqualTo(testBook.getBookId()));

        logger.info("TC102 PASSED");
    }

    @Test
    @Order(3)
    @DisplayName("TC103: Find book by ISBN - Should Return Book")
    void testFindBookByIsbn_ShouldReturnBook() throws SQLException {

        Optional<Book> foundBook = bookDAO.findByIsbn(testBook.getIsbn());

        assertThat(foundBook)
                .isPresent()
                .get()
                .satisfies(book -> assertThat(book.getIsbn()).isEqualTo(testBook.getIsbn()));

        logger.info("TC103 PASSED");
    }

    @Test
    @Order(4)
    @DisplayName("TC104: Find all books - Should Return Non-Empty List")
    void testFindAllBooks_ShouldReturnNonEmptyList() throws SQLException {

        List<Book> books = bookDAO.findAll();

        assertThat(books)
                .isNotNull()
                .isNotEmpty();

        logger.info("TC104 PASSED");
    }

    @Test
    @Order(5)
    @DisplayName("TC105: Decrease available copies - Should Success")
    void testDecreaseAvailableCopies_ShouldSuccess() throws SQLException {

        int original = bookDAO.findById(testBook.getBookId()).get().getAvailableCopies();

        boolean decreased = bookDAO.decreaseAvailableCopies(testBook.getBookId());

        assertThat(decreased).isTrue();

        int after = bookDAO.findById(testBook.getBookId()).get().getAvailableCopies();

        assertThat(after).isEqualTo(original - 1);

        logger.info("TC105 PASSED");
    }

    @Test
    @Order(6)
    @DisplayName("TC106: Increase available copies - Should Success")
    void testIncreaseAvailableCopies_ShouldSuccess() throws SQLException {

        int original = bookDAO.findById(testBook.getBookId()).get().getAvailableCopies();

        boolean increased = bookDAO.increaseAvailableCopies(testBook.getBookId());

        assertThat(increased).isTrue();

        int after = bookDAO.findById(testBook.getBookId()).get().getAvailableCopies();

        assertThat(after).isEqualTo(original + 1);

        logger.info("TC106 PASSED");
    }

    @Test
    @Order(7)
    @DisplayName("TC107: Search books by title - Should Return Matching Books")
    void testSearchByTitle_ShouldReturnMatchingBooks() throws SQLException {

        String uniqueTitle = "UniqueSearchTestBook" + System.currentTimeMillis();
        Book searchBook = createTestBook();
        searchBook.setTitle(uniqueTitle);

        searchBook = bookDAO.create(searchBook);
        createdBookIds.add(searchBook.getBookId());

        List<Book> found = bookDAO.searchByTitle("SearchTest");

        assertThat(found)
                .isNotEmpty()
                .anySatisfy(book -> assertThat(book.getTitle()).containsIgnoringCase("SearchTest"));

        logger.info("TC107 PASSED");
    }

    @Test
    @Order(8)
    @DisplayName("TC108: Find available books - Should Return Only Available")
    void testFindAvailableBooks_ShouldReturnOnlyAvailable() throws SQLException {

        List<Book> books = bookDAO.findAvailableBooks();

        assertThat(books)
                .allSatisfy(book -> assertThat(book.getAvailableCopies()).isGreaterThan(0));

        logger.info("TC108 PASSED");
    }

    @Test
    @Order(9)
    @DisplayName("TC109: Count all books - Should Return Correct Count")
    void testCountAllBooks_ShouldReturnCorrectCount() throws SQLException {

        int total = bookDAO.countAll();
        int available = bookDAO.countAvailableBooks();

        assertThat(total).isGreaterThanOrEqualTo(1);
        assertThat(available).isLessThanOrEqualTo(total);

        logger.info("TC109 PASSED");
    }

    // ------------------------------------------------------------------------------------------------ //
    // NEGATIVE TEST CASES
    // ------------------------------------------------------------------------------------------------ //

    @Test
    @Order(20)
    @DisplayName("TC120: Create duplicate ISBN - Should Fail")
    void testCreateBook_DuplicateIsbn_ShouldFail() {

        Book duplicate = createTestBook();
        duplicate.setIsbn(testBook.getIsbn());

        assertThatThrownBy(() -> bookDAO.create(duplicate))
                .isInstanceOf(SQLException.class);

        logger.info("TC120 PASSED");
    }

    @Test
    @Order(21)
    @DisplayName("TC121: Create invalid available copies - Should Fail")
    void testCreateBook_InvalidAvailableCopies_ShouldFail() {

        Book invalid = createTestBook();
        invalid.setTotalCopies(5);
        invalid.setAvailableCopies(10);

        assertThatThrownBy(() -> bookDAO.create(invalid))
                .isInstanceOf(SQLException.class);

        logger.info("TC121 PASSED");
    }

    @Test
    @Order(22)
    @DisplayName("TC122: Create invalid publication year - Should Fail")
    void testCreateBook_InvalidYear_ShouldFail() {

        Book invalid = createTestBook();
        invalid.setPublicationYear(999);

        assertThatThrownBy(() -> bookDAO.create(invalid))
                .isInstanceOf(SQLException.class);

        logger.info("TC122 PASSED");
    }

    @Test
    @Order(23)
    @DisplayName("TC123: Decrease copies when zero - Should Fail")
    void testDecreaseAvailableCopies_WhenZero_ShouldFail() throws SQLException {

        bookDAO.updateAvailableCopies(testBook.getBookId(), 0);

        boolean decreased = bookDAO.decreaseAvailableCopies(testBook.getBookId());

        assertThat(decreased).isFalse();

        bookDAO.updateAvailableCopies(testBook.getBookId(), 5);

        logger.info("TC123 PASSED");
    }

    @Test
    @Order(24)
    @DisplayName("TC124: Increase copies when at max - Should Fail")
    void testIncreaseAvailableCopies_WhenAtTotal_ShouldFail() throws SQLException {

        int total = bookDAO.findById(testBook.getBookId()).get().getTotalCopies();

        bookDAO.updateAvailableCopies(testBook.getBookId(), total);

        boolean increased = bookDAO.increaseAvailableCopies(testBook.getBookId());

        assertThat(increased).isFalse();

        bookDAO.updateAvailableCopies(testBook.getBookId(), 5);

        logger.info("TC124 PASSED");
    }

    @Test
    @Order(25)
    @DisplayName("TC125: Find non-existent ID - Should Return Empty")
    void testFindBookById_NonExistent_ShouldReturnEmpty() throws SQLException {

        assertThat(bookDAO.findById(999999)).isEmpty();

        logger.info("TC125 PASSED");
    }

    @Test
    @Order(26)
    @DisplayName("TC126: Delete non-existent book - Should Return False")
    void testDelete_NonExistent_ShouldReturnFalse() throws SQLException {

        assertThat(bookDAO.delete(999999)).isFalse();

        logger.info("TC126 PASSED");
    }

    // ------------------------------------------------------------------------------------------------ //
    // BOUNDARY TEST CASES
    // ------------------------------------------------------------------------------------------------ //

    @Test
    @Order(30)
    @DisplayName("TC130: Max-length ISBN - Should Success")
    void testCreateBook_MaxIsbn_ShouldSuccess() throws SQLException {

        Book b = createTestBook();
        b.setIsbn("9".repeat(13));

        Book created = bookDAO.create(b);
        createdBookIds.add(created.getBookId());

        assertThat(created.getIsbn()).hasSize(13);

        logger.info("TC130 PASSED");
    }

    @Test
    @Order(31)
    @DisplayName("TC131: Max-length title - Should Success")
    void testCreateBook_MaxTitle_ShouldSuccess() throws SQLException {

        Book b = createTestBook();
        b.setTitle("T".repeat(200));

        Book created = bookDAO.create(b);
        createdBookIds.add(created.getBookId());

        assertThat(created.getTitle()).hasSize(200);

        logger.info("TC131 PASSED");
    }

    // ------------------------------------------------------------------------------------------------ //
    // PERFORMANCE TEST
    // ------------------------------------------------------------------------------------------------ //

    @Test
    @Order(40)
    @DisplayName("TC140: Search performance test < 200ms")
    void testSearchPerformance() throws SQLException {

        long total = 0;

        for (int i = 0; i < 10; i++) {
            long start = System.nanoTime();
            bookDAO.searchByTitle("Test");
            long end = System.nanoTime();
            total += (end - start);
        }

        long avgMs = (total / 10) / 1_000_000;

        assertThat(avgMs).isLessThan(200);

        logger.info("TC140 PASSED: Average = " + avgMs + "ms");
    }

    // ------------------------------------------------------------------------------------------------ //
    // HELPER
    // ------------------------------------------------------------------------------------------------ //

    private Book createTestBook() {
        return Book.builder()
                .isbn("9" + System.currentTimeMillis())
                .title(faker.book().title() + " Test Book")
                .authorId(1)
                .publisherId(1)
                .categoryId(1)
                .publicationYear(2023)
                .pages(300)
                .language("Indonesian")
                .description("Test book description for automated testing")
                .totalCopies(5)
                .availableCopies(5)
                .price(new java.math.BigDecimal("75000.00"))
                .location("Rak A-1")
                .status("available")
                .build();
    }
}
