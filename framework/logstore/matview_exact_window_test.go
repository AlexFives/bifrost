package logstore

import (
	"fmt"
	"os"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestExactTimeWindowDisablesHourlyMaterializedView(t *testing.T) {
	filters := SearchFilters{VirtualKeyIDs: []string{"vk-one"}}
	require.True(t, canUseMatViewFilters(filters))
	filters.RequireExactTimeWindow = true
	require.False(t, canUseMatViewFilters(filters))
}

// An explicit DSN is required so this test never modifies a developer's default database.
func TestExactModelRankingsWithPostgres(t *testing.T) {
	dsn := os.Getenv("BIFROST_KEY_USAGE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("BIFROST_KEY_USAGE_TEST_POSTGRES_DSN not set")
	}
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	adminSQL, err := admin.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = adminSQL.Close() })
	schema := fmt.Sprintf("key_usage_%d", time.Now().UnixNano())
	require.NoError(t, admin.Exec("CREATE SCHEMA "+schema).Error)
	t.Cleanup(func() { _ = admin.Exec("DROP SCHEMA " + schema + " CASCADE").Error })
	db, err := gorm.Open(postgres.Open(dsn+" search_path="+schema), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, db.AutoMigrate(&Log{}))

	end := time.Now().UTC().Truncate(time.Hour).Add(-20 * time.Minute)
	start := end.Add(-25 * time.Hour)
	key, otherKey := "vk-one", "vk-two"
	insert := func(id, model, vk string, timestamp time.Time) {
		require.NoError(t, db.Create(&Log{ID: id, Model: model, Provider: "openai", Object: "chat_completion", Status: "success", Timestamp: timestamp, VirtualKeyID: &vk}).Error)
	}
	insert("outside", "outside-window", key, start.Add(-10*time.Minute))
	insert("inside", "inside-window", key, start.Add(time.Minute))
	insert("other", "other-key", otherKey, start.Add(time.Minute))
	require.NoError(t, db.Exec(mvLogsHourlyDDL).Error)
	insert("recent", "recent-unrefreshed", key, end.Add(-time.Minute))

	store := &RDBLogStore{db: db, logger: bifrost.NewDefaultLogger(schemas.LogLevelInfo)}
	store.matViewsReady.Store(true)
	filters := SearchFilters{VirtualKeyIDs: []string{key}, StartTime: &start, EndTime: &end}
	approximate, err := store.GetModelRankings(t.Context(), filters)
	require.NoError(t, err)
	approximateModels := make(map[string]bool)
	for _, entry := range approximate.Rankings {
		approximateModels[entry.Model] = true
	}
	require.True(t, approximateModels["outside-window"])
	require.False(t, approximateModels["recent-unrefreshed"])
	filters.RequireExactTimeWindow = true
	exact, err := store.GetModelRankings(t.Context(), filters)
	require.NoError(t, err)
	models := make(map[string]bool)
	for _, entry := range exact.Rankings {
		models[entry.Model] = true
	}
	require.Equal(t, map[string]bool{"inside-window": true, "recent-unrefreshed": true}, models)
}
