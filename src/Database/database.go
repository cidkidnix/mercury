package Database

import (
  "gorm.io/gorm"
  "gorm.io/driver/sqlite"
  //"gorm.io/gorm/logger"
  //"github.com/digital-asset/dazl-client/v7/go/api/com/daml/ledger/api/v1"
)


type LastOffset struct {
    Id int32
    Offset string `gorm:"index:offset_idx_offset,unique"`
}

type ContractTable struct {
    ContractID string `gorm:"index:contracts_idx_contract_id,unique"`
    ContractKey []byte
    CreateArguments []byte
    TemplateFqn string
    Witnesses []byte
    Observers []byte
    Offset string
}

func (ContractTable) TableName() string {
  return "__contracts"
}

type ArchivesTable struct {
    ContractID string `gorm:"index:archives_idx_contract_id,unique"`
    Offset string
}

func (ArchivesTable) TableName() string {
  return "__archives"
}

type CreatesTable struct {
   ContractID string `gorm:"index:creates_idx_contract_id,unique"`
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
  db.AutoMigrate(&LastOffset{})
  return db
}
