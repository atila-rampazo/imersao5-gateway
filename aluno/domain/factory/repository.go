package factory

import "github.com/atila-rampazo/imersao5-gateway/domain/repository"

type RepositoryFactory interface {
	CreateTransactionRepository() repository.TransactionRepository
}
