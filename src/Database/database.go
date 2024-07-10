package Database

import (
  "gorm.io/gorm"
  "gorm.io/driver/sqlite"
  //"github.com/digital-asset/dazl-client/v7/go/api/com/daml/ledger/api/v1"
)

type ContractTable struct {
    ContractID string `gorm:"uniqueIndex"`
    ContractKey []byte
    //`gorm:"uniqueIndex:idx_contract_key"`
    CreateArguments []byte
    TemplateFqn string
    Witnesses []byte
    Observers []byte

}

func (ContractTable) TableName() string {
  return "__contracts"
}

type ArchivesTable struct {
    ContractID string `gorm:"uniqueIndex"`
}

func (ArchivesTable) TableName() string {
  return "__archives"
}

type CreatesTable struct {
   ContractID string `gorm:"uniqueIndex"`
}

func (CreatesTable) TableName() string {
  return "__creates"
}

func InitializeDB() (db *gorm.DB) {
  db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
  if err != nil {
    panic("Failed to open DB")
  }

  db.AutoMigrate(&ContractTable{})
  db.AutoMigrate(&ArchivesTable{})
  db.AutoMigrate(&CreatesTable{})
  return db
}
