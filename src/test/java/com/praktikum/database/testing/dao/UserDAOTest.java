package com.praktikum.database.testing.dao;

// Import classes untuk testing
import com.github.javafaker.Faker;
import com.praktikum.database.testing.BaseDatabaseTest;
import com.praktikum.database.testing.model.User;
import org.junit.jupiter.api.*;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

// Import static assertions untuk readable test code
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive test suite untuk UserDAO
 * Menguji semua operasi CRUD dari berbagai scenarios
 * Menggunakan AssertJ untuk fluent assertions
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("UserDAO CRUD Operations Test Suite")
public class UserDAOTest extends BaseDatabaseTest {

    private static UserDAO userDAO;
    private static Faker faker;

    private static User testUser;
    private static List<Integer> createdUserIds;

    @BeforeAll
    static void setUpAll() {
        logger.info("Starting UserDAO CRUD Tests");

        userDAO = new UserDAO();
        faker = new Faker();
        createdUserIds = new java.util.ArrayList<>();
    }

    @AfterAll
    static void tearDownAll() throws SQLException {
        logger.info("UserDAO CRUD Tests Completed");

        for (Integer userId : createdUserIds) {
            try {
                userDAO.delete(userId);
                logger.fine("Cleaned up test user ID: " + userId);
            } catch (SQLException e) {
                logger.warning("Gagal cleanup user ID: " + userId + " - " + e.getMessage());
            }
        }
    }

    // =========================================================================
    // POSITIVE TEST CASES
    // =========================================================================

    @Test
    @Order(1)
    @DisplayName("TC001: Create user dengan data valid - Should Success")
    void testCreateUser_WithValidData_ShouldSuccess() throws SQLException {
        testUser = createTestUser();

        User createdUser = userDAO.create(testUser);
        createdUserIds.add(createdUser.getUserId());

        assertThat(createdUser)
                .isNotNull()
                .satisfies(user -> {
                    assertThat(user.getUserId()).isNotNull().isPositive();
                    assertThat(user.getUsername()).isEqualTo(testUser.getUsername());
                    assertThat(user.getEmail()).isEqualTo(testUser.getEmail());
                    assertThat(user.getFullName()).isEqualTo(testUser.getFullName());
                    assertThat(user.getRole()).isEqualTo("member");
                    assertThat(user.getStatus()).isEqualTo("active");
                    assertThat(user.getCreatedAt()).isNotNull();
                    assertThat(user.getUpdatedAt()).isNotNull();
                });

        logger.info("TC001 PASSED: User created dengan ID: " + createdUser.getUserId());
    }

    @Test
    @Order(2)
    @DisplayName("TC002: Find user by existing ID - Should Return User")
    void testFindUserById_WithExistingId_ShouldReturnUser() throws SQLException {
        Optional<User> foundUser = userDAO.findById(testUser.getUserId());

        assertThat(foundUser)
                .isPresent()
                .get()
                .satisfies(user -> {
                    assertThat(user.getUserId()).isEqualTo(testUser.getUserId());
                    assertThat(user.getUsername()).isEqualTo(testUser.getUsername());
                    assertThat(user.getEmail()).isEqualTo(testUser.getEmail());
                });

        logger.info("TC002 PASSED: User found dengan ID: " + testUser.getUserId());
    }

    @Test
    @Order(3)
    @DisplayName("TC003: Find user by username - Should Return User")
    void testFindByUsername_WithExistingUsername_ShouldReturnUser() throws SQLException {
        Optional<User> foundUser = userDAO.findByUsername(testUser.getUsername());

        assertThat(foundUser)
                .isPresent()
                .get()
                .satisfies(user -> assertThat(user.getUsername()).isEqualTo(testUser.getUsername()));

        logger.info("TC003 PASSED: User found dengan username: " + testUser.getUsername());
    }

    @Test
    @Order(4)
    @DisplayName("TC004: Find all users - Should Return Non-Empty List")
    void testFindAllUsers_ShouldReturnNonEmptyList() throws SQLException {
        List<User> users = userDAO.findAll();

        assertThat(users)
                .isNotNull()
                .isNotEmpty()
                .allSatisfy(user -> {
                    assertThat(user.getUserId()).isNotNull();
                    assertThat(user.getUsername()).isNotBlank();
                    assertThat(user.getEmail()).isNotBlank();
                });

        logger.info("TC004 PASSED: Found " + users.size() + " users");
    }

    @Test
    @Order(5)
    @DisplayName("TC005: Update user email - Should Success")
    void testUpdateUserEmail_ShouldSuccess() throws SQLException {
        String newEmail = "updated." + faker.internet().emailAddress();
        testUser.setEmail(newEmail);

        boolean updated = userDAO.update(testUser);
        assertThat(updated).isTrue();

        Optional<User> updatedUser = userDAO.findById(testUser.getUserId());
        assertThat(updatedUser)
                .isPresent()
                .get()
                .satisfies(user -> assertThat(user.getEmail()).isEqualTo(newEmail));

        logger.info("TC005 PASSED: User email updated to: " + newEmail);
    }

    @Test
    @Order(6)
    @DisplayName("TC006: Update user last login - Should Success")
    void testUpdateUserLastLogin_ShouldSuccess() throws SQLException {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        testUser.setLastLogin(now);

        boolean updated = userDAO.update(testUser);
        assertThat(updated).isTrue();

        Optional<User> updatedUser = userDAO.findById(testUser.getUserId());
        assertThat(updatedUser)
                .isPresent()
                .get()
                .satisfies(user -> assertThat(user.getLastLogin()).isNotNull());

        logger.info("TC006 PASSED: User last login updated");
    }

    @Test
    @Order(7)
    @DisplayName("TC007: Auto-update trigger - updated_at should change on update")
    void testAutoUpdateTrigger_UpdatedAtShouldChangeOnUpdate() throws SQLException, InterruptedException {
        Optional<User> beforeUpdate = userDAO.findById(testUser.getUserId());
        Timestamp originalUpdatedAt = beforeUpdate.get().getUpdatedAt();

        pause(2000);

        testUser.setFullName("Updated Name for Trigger Test");
        userDAO.update(testUser);

        Optional<User> afterUpdate = userDAO.findById(testUser.getUserId());
        assertThat(afterUpdate)
                .isPresent()
                .get()
                .satisfies(user -> {
                    assertThat(user.getUpdatedAt()).isAfter(originalUpdatedAt);
                    assertThat(user.getFullName()).isEqualTo("Updated Name for Trigger Test");
                });

        logger.info("TC007 PASSED: Trigger updated_at working correctly");
    }

    @Test
    @Order(8)
    @DisplayName("TC008: Delete existing user - Should Success")
    void testDeleteUser_WithExistingUser_ShouldSuccess() throws SQLException {
        User userToDelete = createTestUser();
        userToDelete = userDAO.create(userToDelete);

        boolean deleted = userDAO.delete(userToDelete.getUserId());
        assertThat(deleted).isTrue();

        Optional<User> deletedUser = userDAO.findById(userToDelete.getUserId());
        assertThat(deletedUser).isEmpty();

        logger.info("TC008 PASSED: User deleted successfully");
    }

    // =========================================================================
    // NEGATIVE TEST CASES
    // =========================================================================

    @Test
    @Order(10)
    @DisplayName("TC010: Create user dengan duplicate username - Should Fail")
    void testCreateUser_WithDuplicateUsername_ShouldFail() {
        User duplicateUser = createTestUser();
        duplicateUser.setUsername(testUser.getUsername());

        assertThatThrownBy(() -> userDAO.create(duplicateUser))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("duplicate")
                .hasMessageContaining("username");

        logger.info("TC010 PASSED: Duplicate username constraint works");
    }

    @Test
    @Order(11)
    @DisplayName("TC011: Create user dengan duplicate email - Should Fail")
    void testCreateUser_WithDuplicateEmail_ShouldFail() {
        User duplicateUser = createTestUser();
        duplicateUser.setEmail(testUser.getEmail());

        assertThatThrownBy(() -> userDAO.create(duplicateUser))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("duplicate")
                .hasMessageContaining("email");

        logger.info("TC011 PASSED: Duplicate email constraint works");
    }

    @Test
    @Order(12)
    @DisplayName("TC012: Create user dengan invalid role - Should Fail")
    void testCreateUser_WithInvalidRole_ShouldFail() {
        User invalidUser = createTestUser();
        invalidUser.setRole("superadmin");

        assertThatThrownBy(() -> userDAO.create(invalidUser))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("check");

        logger.info("TC012 PASSED: Role check constraint works");
    }

    @Test
    @Order(13)
    @DisplayName("TC013: Create user dengan NULL username - Should Fail")
    void testCreateUser_WithNullUsername_ShouldFail() {
        User nullUser = createTestUser();
        nullUser.setUsername(null);

        assertThatThrownBy(() -> userDAO.create(nullUser))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("null");

        logger.info("TC013 PASSED: NOT NULL constraint works");
    }

    @Test
    @Order(14)
    @DisplayName("TC014: Find user dengan non-existent ID - Should Return Empty")
    void testFindUserById_WithNonExistentId_ShouldReturnEmpty() throws SQLException {
        Optional<User> foundUser = userDAO.findById(999999);
        assertThat(foundUser).isEmpty();

        logger.info("TC014 PASSED: Non-existent ID handled correctly");
    }

    @Test
    @Order(15)
    @DisplayName("TC015: Delete non-existent user - Should Return False")
    void testDeleteUser_WithNonExistentUser_ShouldReturnFalse() throws SQLException {
        boolean deleted = userDAO.delete(999999);
        assertThat(deleted).isFalse();

        logger.info("TC015 PASSED: Non-existent user delete handled correctly");
    }

    // =========================================================================
    // BOUNDARY TEST CASES
    // =========================================================================

    @Test
    @Order(20)
    @DisplayName("TC020: Create user dengan username max length - Should Success")
    void testCreateUser_WithMaxLengthUsername_ShouldSuccess() throws SQLException {
        String maxUsername = "a".repeat(50);
        User user = createTestUser();
        user.setUsername(maxUsername);

        User createdUser = userDAO.create(user);
        createdUserIds.add(createdUser.getUserId());

        assertThat(createdUser.getUsername()).hasSize(50);

        logger.info("TC020 PASSED: Max length username accepted");
    }

    @Test
    @Order(21)
    @DisplayName("TC021: Create user dengan username terlalu panjang - Should Fail")
    void testCreateUser_WithOversizedUsername_ShouldFail() {
        String longUsername = "a".repeat(51);
        User user = createTestUser();
        user.setUsername(longUsername);

        assertThatThrownBy(() -> userDAO.create(user))
                .isInstanceOf(SQLException.class);

        logger.info("TC021 PASSED: Username length constraint enforced");
    }

    // =========================================================================
    // PERFORMANCE TEST
    // =========================================================================

    @Test
    @Order(30)
    @DisplayName("TC030: Find user by ID - Performance (< 100ms)")
    void testFindUserById_Performance() throws SQLException {
        int iterations = 10;
        long totalTime = 0;

        for (int i = 0; i < iterations; i++) {
            long start = System.nanoTime();
            userDAO.findById(testUser.getUserId());
            long end = System.nanoTime();
            totalTime += (end - start);
        }

        long avgMs = (totalTime / iterations) / 1_000_000;
        assertThat(avgMs).isLessThan(100);

        logger.info("TC030 PASSED: Avg query time = " + avgMs + " ms");
    }

    // =========================================================================
    // HELPER METHODS
    // =========================================================================

    private User createTestUser() {
        return User.builder()
                .username("testuser_" + System.currentTimeMillis() + "_" + faker.number().randomNumber())
                .email(faker.internet().emailAddress())
                .fullName(faker.name().fullName())
                .phone(faker.phoneNumber().cellPhone())
                .role("member")
                .status("active")
                .build();
    }
}
